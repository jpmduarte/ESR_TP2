#!/usr/bin/env python3
import sys
import json
import socket
import threading
import time
import heapq
import base64
from datetime import datetime
from collections import deque


###############################################################################
# CONFIG
###############################################################################

PING_INTERVAL = 2       # seconds between RTT probes
LSA_INTERVAL = 10       # periodic LSA regeneration
SERVER_PORT = 5000      # port for REGISTER on server
RECV_BUF = 4096
RTT_THRESHOLD = 0.005   # 5ms - only flood LSA if RTT changes by more than this
PEER_TIMEOUT = 30       # seconds before declaring peer dead
MAX_LSA_AGE = 60        # seconds before LSA expires


###############################################################################
# UTILS
###############################################################################

def log(nid, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}][{nid}] {msg}")


def send_json(sock, obj):
    try:
        data = json.dumps(obj) + "\n"
        sock.sendall(data.encode())
        return True
    except:
        return False


def recv_lines(sock, buffer):
    """Receive data and return (lines, new_buffer)"""
    try:
        data = sock.recv(RECV_BUF)
        if not data:
            return None, buffer
        buffer += data.decode()
        lines = []
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if line.strip():
                lines.append(line)
        return lines, buffer
    except:
        return None, buffer


###############################################################################
# NODE CLASS
###############################################################################

class OTTNode:
    def __init__(self, node_id, mode):
        self.node_id = node_id
        self.mode = mode
        self.listen_port = 6000 + int(node_id[1:])

        # Static neighbors (server defines)
        self.neighbors_cfg = []

        # Active neighbors (TCP) - FIX #1: Track connection direction
        self.sock_peers = {}        # nid -> {"sock", "ip", "port", "direction": "in"/"out"}
        self.peer_buffers = {}
        
        # FIX #1: Track last contact time for timeout detection
        self.peer_last_seen = {}    # nid -> timestamp

        # RTT to neighbors - FIX #3: Use EWMA for dampening
        self.rtt = {}               # nid -> float
        self.rtt_smooth = {}        # nid -> smoothed RTT
        self.rtt_alpha = 0.3        # EWMA smoothing factor

        # LSA state
        self.seqno = 0
        self.lsdb = {}             # nid -> {"seq", "neighbors", "timestamp"}
        self.last_lsa_content = {}  # FIX #4: Track last LSA content to avoid redundant floods

        # Server only
        self.topology = {}
        self.client_map = {}
        self.registered_nodes = {}

        self.lock = threading.Lock()
        self.running = True


    ###########################################################################
    # LOAD TOPOLOGY (SERVER ONLY)
    ###########################################################################
    def load_topology(self, topo_file):
        try:
            with open(topo_file) as f:
                data = json.load(f)

            self.topology = data["nodes"]
            self.client_map = data.get("clients", {})

            log(self.node_id, f"Topology loaded ({len(self.topology)} nodes)")

            if self.node_id in self.topology:
                for nb in self.topology[self.node_id]:
                    self.neighbors_cfg.append({
                        "id": nb,
                        "ip": None,
                        "port": 6000 + int(nb[1:])
                    })

            return True
        except Exception as e:
            log(self.node_id, f"ERROR loading topology: {e}")
            return False


    ###########################################################################
    # SERVER: ACCEPT REGISTRATIONS (TCP 5000)
    ###########################################################################
    def server_registration_loop(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", SERVER_PORT))
        srv.listen(50)
        log(self.node_id, f"Accepting registrations on :{SERVER_PORT}")

        while self.running:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_registration, args=(conn,), daemon=True).start()

    def handle_registration(self, conn):
        buffer = ""
        try:
            while True:
                lines, buffer = recv_lines(conn, buffer)
                if lines is None:
                    break

                for line in lines:
                    msg = json.loads(line)

                    if msg["type"] == "REGISTER":
                        nid = msg["node_id"]
                        ip = msg["ip"]
                        port = msg["port"]

                        with self.lock:
                            self.registered_nodes[nid] = {"ip": ip, "port": port}

                        log(self.node_id, f"REGISTERED {nid} at {ip}:{port}")

                        neighs = []
                        for nb in self.topology.get(nid, []):
                            if nb in self.registered_nodes:
                                neighs.append({
                                    "id": nb,
                                    "ip": self.registered_nodes[nb]["ip"],
                                    "port": self.registered_nodes[nb]["port"]
                                })

                        send_json(conn, {"type": "NEIGHBORS", "neighbors": neighs})

                        for nb in self.neighbors_cfg:
                            if nb["id"] == nid:
                                nb["ip"] = ip
        except:
            pass
        finally:
            conn.close()


    ###########################################################################
    # RELAY: REGISTER WITH SERVER
    ###########################################################################
    def relay_register(self, server_ip):
        try:
            sock = socket.socket()
            sock.connect((server_ip, SERVER_PORT))
            my_ip = sock.getsockname()[0]

            send_json(sock, {
                "type": "REGISTER",
                "node_id": self.node_id,
                "ip": my_ip,
                "port": self.listen_port
            })

            buffer = ""
            while True:
                lines, buffer = recv_lines(sock, buffer)
                if lines is None:
                    break
                for line in lines:
                    msg = json.loads(line)
                    if msg["type"] == "NEIGHBORS":
                        self.neighbors_cfg = msg["neighbors"]
                        log(self.node_id, f"Neighbors: {self.neighbors_cfg}")
                        sock.close()
                        return True

            sock.close()
            return True
        except Exception as e:
            log(self.node_id, f"ERROR relay registration: {e}")
            return False


    ###########################################################################
    # OVERLAY TCP LISTENING
    ###########################################################################
    def listen_overlay(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.listen_port))
        srv.listen(20)
        log(self.node_id, f"Listening for neighbors on :{self.listen_port}")

        while self.running:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()


    ###########################################################################
    # CONNECT TO NEIGHBORS
    ###########################################################################
    def connect_neighbors_loop(self):
        while self.running:
            with self.lock:
                cfg_copy = list(self.neighbors_cfg)

            for nb in cfg_copy:
                nid = nb["id"]
                ip = nb.get("ip")
                port = nb.get("port")

                if not ip:
                    continue

                with self.lock:
                    if nid in self.sock_peers:
                        continue

                # FIX #1: Only connect if we have lower node_id (deterministic rule)
                # Extract numeric part for proper comparison (O10 > O2)
                my_num = int(self.node_id[1:])
                their_num = int(nid[1:])
                if my_num > their_num:
                    continue

                try:
                    s2 = socket.socket()
                    s2.connect((ip, port))

                    with self.lock:
                        self.sock_peers[nid] = {
                            "sock": s2, 
                            "ip": ip, 
                            "port": port,
                            "direction": "out"  # FIX #1: Mark as outbound
                        }
                        self.peer_buffers[nid] = ""
                        self.peer_last_seen[nid] = time.time()

                    threading.Thread(
                        target=self.handle_connected_peer,
                        args=(nid, s2),
                        daemon=True
                    ).start()

                    log(self.node_id, f"Connected to {nid}")

                except:
                    pass

            time.sleep(2)


    ###########################################################################
    # CONNECTION HANDLERS
    ###########################################################################
    def handle_connection(self, sock):
        """Someone connected to me (inbound)."""
        try:
            remote_ip, remote_port = sock.getpeername()
        except:
            return

        nid = None
        buffer = ""
        
        try:
            while nid is None:
                lines, buffer = recv_lines(sock, buffer)
                if lines is None:
                    return
                for line in lines:
                    msg = json.loads(line)
                    if msg["type"] == "HELLO" and "node_id" in msg:
                        nid = msg["node_id"]
                        
                        # FIX #1: Duplicate connection detection
                        with self.lock:
                            if nid in self.sock_peers:
                                # Already have connection - apply deterministic rule
                                # Extract numeric part for proper comparison
                                my_num = int(self.node_id[1:])
                                their_num = int(nid[1:])
                                
                                if my_num < their_num:
                                    # We should be the connector, reject this inbound
                                    log(self.node_id, f"Rejecting duplicate inbound from {nid}")
                                    sock.close()
                                    return
                                else:
                                    # Close the old outbound, accept this inbound
                                    log(self.node_id, f"Replacing outbound with inbound from {nid}")
                                    old_sock = self.sock_peers[nid]["sock"]
                                    try:
                                        old_sock.close()
                                    except:
                                        pass
                            
                            self.sock_peers[nid] = {
                                "sock": sock, 
                                "ip": remote_ip, 
                                "port": remote_port,
                                "direction": "in"  # FIX #1: Mark as inbound
                            }
                            self.peer_buffers[nid] = buffer
                            self.peer_last_seen[nid] = time.time()
                        
                        break

            log(self.node_id, f"Accepted connection from {nid}")
            self.recv_loop(nid, sock)

        except:
            pass
        finally:
            self.remove_peer(nid)
            try:
                sock.close()
            except:
                pass


    def handle_connected_peer(self, nid, sock):
        """We connected to a neighbor - send HELLO first."""
        send_json(sock, {"type": "HELLO", "node_id": self.node_id})
        self.recv_loop(nid, sock)


    ###########################################################################
    # GENERIC RECEIVE LOOP FOR TCP MESSAGES
    ###########################################################################
    def recv_loop(self, nid, sock):
        buffer = ""
        while self.running:
            lines, buffer = recv_lines(sock, buffer)
            if lines is None:
                break

            # Update last seen time
            with self.lock:
                self.peer_last_seen[nid] = time.time()

            for line in lines:
                try:
                    msg = json.loads(line)
                    self.process_message(nid, sock, msg)
                except:
                    pass

        self.remove_peer(nid)


    ###########################################################################
    # REMOVE PEER (TCP DEAD)
    ###########################################################################
    def remove_peer(self, nid):
        if nid:
            with self.lock:
                if nid in self.sock_peers:
                    log(self.node_id, f"Peer {nid} disconnected")
                    del self.sock_peers[nid]
                    self.rtt.pop(nid, None)
                    self.rtt_smooth.pop(nid, None)
                    self.peer_last_seen.pop(nid, None)
            
            # FIX #5: Generate LSA to announce link removal
            self.generate_lsa()

    # =========================================================================
    # FORWARDING & STREAMING LOGIC (DATA PLANE)
    # =========================================================================

    def handle_stream_packet(self, msg):
        """NOVO: Recebe pacote, descodifica e guarda em ficheiro."""
        payload = msg["payload"]
        seq = msg["seq"]
        
        try:
            # Descodificar Base64 de volta para binário
            binary_data = base64.b64decode(payload)
            
            # Guardar em disco (Modo 'ab' = append binary)
            filename = f"received_stream_{self.node_id}.mp4"
            
            with open(filename, "ab") as f:
                f.write(binary_data)
                
            # Log apenas de 100 em 100 pacotes
            if seq % 100 == 0:
                log(self.node_id, f"Recebido pacote {seq} de {msg['src']} - total guardado em {filename}")

        except Exception as e:
            log(self.node_id, f"ERRO ao processar pacote de stream: {e}")
            
            
    def start_streaming(self, target_node, filename="3min.mp4"):
        
        try:
            with open(filename, "rb") as f:
                seq = 1
                while self.running:
                    chunk = f.read(4096) # Lê 4KB por chunk
                    if not chunk:
                        break
                        
                    # Codificar binário para Base64 (para ir dentro do JSON)
                    b64_data = base64.b64encode(chunk).decode('utf-8')
                    
                    packet = {
                        "type": "STREAM_DATA",
                        "src": self.node_id,
                        "dst": target_node,
                        "seq": seq,
                        "payload": b64_data
                    }
                    
                    # 1. Obter o primeiro salto (next_hop)
                    with self.lock:
                        route_info = self.routes.get(target_node)
                    
                    if route_info:
                        next_hop = route_info['next']
                        
                        # 2. Enviar o pacote
                        with self.lock:
                            conn_info = self.sock_peers.get(next_hop)

                        if conn_info:
                            send_json(conn_info['sock'], packet)
                        else:
                            log(self.node_id, f"ERRO: Vizinho {next_hop} desligado, parando stream.")
                            break
                    else:
                        log(self.node_id, f"ERRO: Sem rota ativa para {target_node}")
                        break
                    
                    seq += 1
                    time.sleep(0.01) # Pequena pausa para controlar o débito
                    
            log(self.node_id, "Fim do envio do ficheiro de video.")
            
        except FileNotFoundError:
            log(self.node_id, f"ERRO: Ficheiro de video {filename} não encontrado.")


    ###########################################################################
    # MESSAGE HANDLING
    ###########################################################################
    def process_message(self, nid, sock, msg):
        mtype = msg["type"]

        if mtype == "PING":
            send_json(sock, {"type": "PONG", "timestamp": msg["timestamp"]})

        elif mtype == "PONG":
            ts = msg["timestamp"]
            r = time.time() - ts
            
            # FIX #3: Apply EWMA smoothing
            with self.lock:
                if nid in self.rtt_smooth:
                    self.rtt_smooth[nid] = (self.rtt_alpha * r + 
                                           (1 - self.rtt_alpha) * self.rtt_smooth[nid])
                else:
                    self.rtt_smooth[nid] = r
                
                old_rtt = self.rtt.get(nid, 0)
                self.rtt[nid] = self.rtt_smooth[nid]
                
                # FIX #3: Only generate LSA if change is significant
                if abs(self.rtt[nid] - old_rtt) > RTT_THRESHOLD:
                    should_generate = True
                else:
                    should_generate = False
            
            if should_generate:
                self.generate_lsa()

        elif mtype == "LSA":
            self.apply_lsa(msg, nid)
        elif mtype == "STREAM_DATA":
            dst = msg["dst"]
            
            # 1. CASO CLIENTE: Sou eu o destino final? (O4)
            if dst == self.node_id:
                self.handle_stream_packet(msg)
                
            # 2. CASO RELAY: Tenho de reencaminhar? (O2 ou O3)
            else:
                with self.lock:
                    route_info = self.routes.get(dst)
                
                if route_info:
                    next_hop = route_info["next"]
                    
                    # Verificar se o vizinho está conectado
                    with self.lock:
                        conn_info = self.sock_peers.get(next_hop)
                        
                    if conn_info:
                        # 
                        # Reencaminhar o pacote sem tocar no payload
                        send_json(conn_info["sock"], msg)
                        log(self.node_id, f"FORWARDED STREAM_DATA to {dst} via {next_hop}")
                    else:
                        log(self.node_id, f"DROP: Next hop {next_hop} for {dst} is down. Recalculando rotas...")
                else:
                    log(self.node_id, f"DROP: No active route to {dst}. O Dijkstra nao tem caminho.")


    ###########################################################################
    # PERIODIC PING FOR RTT
    ###########################################################################
    def ping_loop(self):
        while self.running:
            with self.lock:
                peers = list(self.sock_peers.items())

            for nid, info in peers:
                ts = time.time()
                send_json(info["sock"], {
                    "type": "PING",
                    "timestamp": ts
                })

            time.sleep(PING_INTERVAL)


    ###########################################################################
    # PEER TIMEOUT DETECTION
    ###########################################################################
    def timeout_check_loop(self):
        """FIX #5: Detect and remove dead peers"""
        while self.running:
            time.sleep(5)
            now = time.time()
            
            with self.lock:
                dead_peers = []
                for nid, last_seen in list(self.peer_last_seen.items()):
                    if now - last_seen > PEER_TIMEOUT:
                        dead_peers.append(nid)
            
            for nid in dead_peers:
                log(self.node_id, f"Peer {nid} timed out")
                with self.lock:
                    if nid in self.sock_peers:
                        try:
                            self.sock_peers[nid]["sock"].close()
                        except:
                            pass
                self.remove_peer(nid)


    ###########################################################################
    # LSA LOGIC
    ###########################################################################
    def generate_lsa(self):
        """Generate own LSA from RTT table."""
        with self.lock:
            neigh_costs = dict(self.rtt)
            
            # FIX #4: Check if content actually changed
            if neigh_costs == self.last_lsa_content:
                return  # No change, don't flood
            
            self.seqno += 1
            self.last_lsa_content = neigh_costs.copy()
            
            lsa = {
                "type": "LSA",
                "origin": self.node_id,
                "seq": self.seqno,
                "neighbors": neigh_costs
            }

            # Apply to own LSDB
            self.lsdb[self.node_id] = {
                "seq": self.seqno,
                "neighbors": neigh_costs,
                "timestamp": time.time()
            }

        self.flood_lsa(lsa)


    def apply_lsa(self, lsa, from_nid):
        origin = lsa["origin"]
        seq = lsa["seq"]

        changed = False

        with self.lock:
            if origin not in self.lsdb or seq > self.lsdb[origin]["seq"]:
                self.lsdb[origin] = {
                    "seq": seq,
                    "neighbors": lsa["neighbors"],
                    "timestamp": time.time()
                }
                changed = True

        if changed:
            self.flood_lsa(lsa, exclude=from_nid)
            self.compute_routes()


    def flood_lsa(self, lsa, exclude=None):
        """FIX #8: Safer iteration with lock"""
        with self.lock:
            peers = list(self.sock_peers.items())

        for nid, info in peers:
            if nid != exclude:
                send_json(info["sock"], lsa)


    ###########################################################################
    # LSDB CLEANUP
    ###########################################################################
    def cleanup_lsdb(self):
        """FIX #5: Remove stale LSAs"""
        while self.running:
            time.sleep(10)
            now = time.time()
            
            with self.lock:
                stale = []
                for nid, entry in self.lsdb.items():
                    if nid == self.node_id:
                        continue
                    if now - entry.get("timestamp", 0) > MAX_LSA_AGE:
                        stale.append(nid)
                
                for nid in stale:
                    log(self.node_id, f"Removing stale LSA from {nid}")
                    del self.lsdb[nid]
            
            if stale:
                self.compute_routes()


    ###########################################################################
    # DIJKSTRA (USING RTT)
    ###########################################################################
    def compute_routes(self):
        with self.lock:
            # FIX #6: Only include nodes with valid entries
            graph = {}
            for nid in self.lsdb:
                graph[nid] = self.lsdb[nid]["neighbors"]

        dist = {self.node_id: 0}
        prev = {self.node_id: None}

        pq = [(0, self.node_id)]

        while pq:
            cost, u = heapq.heappop(pq)
            if cost > dist.get(u, 1e12):
                continue

            for v, w in graph.get(u, {}).items():
                nd = cost + w
                if v not in dist or nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        # Find next hop for each destination
        routes = {}
        for dst in dist:
            if dst == self.node_id:
                continue
            hop = dst
            # Walk back to find first hop
            while prev.get(hop) != self.node_id:
                hop = prev.get(hop)
                if hop is None:
                    break
            if hop and hop in self.sock_peers:  # FIX #10: Only route to active peers
                routes[dst] = {"next": hop, "cost": dist[dst]}

        log(self.node_id, "ROUTES RECOMPUTED:")
        for d, info in routes.items():
            log(self.node_id, 
                f"{self.node_id} → {d} via {info['next']} "
                f"(cost {info['cost']*1000:.1f} ms)")


    ###########################################################################
    # PERIODIC LSA
    ###########################################################################
    def periodic_lsa(self):
        while self.running:
            time.sleep(LSA_INTERVAL)
            # FIX #3 & #4: Periodic LSA only if there are changes
            # generate_lsa will check if content changed
            self.generate_lsa()


    ###########################################################################
    # RUN MODES
    ###########################################################################
    def run_server(self, topo_file):
        if not self.load_topology(topo_file):
            return

        threading.Thread(target=self.server_registration_loop, daemon=True).start()
        threading.Thread(target=self.listen_overlay, daemon=True).start()
        threading.Thread(target=self.connect_neighbors_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.periodic_lsa, daemon=True).start()
        threading.Thread(target=self.timeout_check_loop, daemon=True).start()
        threading.Thread(target=self.cleanup_lsdb, daemon=True).start()

        time.sleep(3)
        self.generate_lsa()

        while True:
            time.sleep(1)

    def run_relay(self, server_ip):
        if not self.relay_register(server_ip):
            return

        threading.Thread(target=self.listen_overlay, daemon=True).start()
        threading.Thread(target=self.connect_neighbors_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.periodic_lsa, daemon=True).start()
        threading.Thread(target=self.timeout_check_loop, daemon=True).start()
        threading.Thread(target=self.cleanup_lsdb, daemon=True).start()

        time.sleep(3)
        self.generate_lsa()

        while True:
            time.sleep(1)


###############################################################################
# MAIN
###############################################################################

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("USAGE:")
        print("  Server: python3 ott_node.py server <node_id> <topology.json>")
        print("  Relay:  python3 ott_node.py relay <node_id> <server_ip>")
        sys.exit(1)

    mode = sys.argv[1]
    node_id = sys.argv[2]
    
    node = OTTNode(node_id, mode)
    
    if mode == "server":
        topo_file = sys.argv[3]
        
        # Chamada para iniciar o stream (No O1)
        def start_test_stream():
            time.sleep(15)
            node.start_streaming("O4", "3min.mp4") # Enviar para o O4 (onde está o cliente C1)
            
        threading.Thread(target=start_test_stream, daemon=True).start()
        node.run_server(topo_file)

    elif mode == "relay":
        server_ip = sys.argv[3]
        node.run_relay(server_ip)