import socket
import json
import threading
import time
import subprocess
import re
import struct

SERVER_IP = '10.0.0.10'
SERVER_PORT = 5000
UDP_PORT = 6001
INGEST_PORT = 6500
KEEPALIVE_INTERVAL = 5

node_id = None
neighbors = {}  # {neighbor_id: {ip, port, conn}}
routes = {}  # {video_id: {source, metric, prev_hop, next_hops[], active, group, port}}
seen_messages = set()  # Para evitar loops no flooding
lock = threading.Lock()

# Conexões TCP com vizinhos
neighbor_connections = {}  # {neighbor_id: socket}


def send_json(sock, msg):
    """Envia mensagem JSON com delimitador."""
    try:
        sock.send((json.dumps(msg) + "\n").encode())
        return True
    except Exception as e:
        print(f"[ERROR] send_json failed: {e}")
        return False


def get_ip_for_destination(dest_ip: str) -> str:
    """Descobre IP local usado para comunicar com destino."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((dest_ip, 9))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        pass
    
    try:
        out = subprocess.check_output(f"ip -4 route get {dest_ip}", shell=True).decode()
        m = re.search(r'\bsrc\s+(\d+\.\d+\.\d+\.\d+)', out)
        if m:
            return m.group(1)
    except Exception:
        pass
    
    return "127.0.0.1"


def udp_listener():
    """Responde a PING com PONG para medição de RTT."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', UDP_PORT))
    print(f"[UDP] Listener active on port {UDP_PORT}")
    
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            if data == b'PING':
                sock.sendto(b'PONG', addr)
        except Exception as e:
            print(f"[UDP][ERROR] {e}")


def probe_nodes(server_sock, targets):
    """Mede RTT UDP para lista de nós."""
    results = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1.0)
    
    for target in targets:
        start = time.time()
        try:
            sock.sendto(b'PING', (target['ip'], UDP_PORT))
            data, _ = sock.recvfrom(1024)
            if data == b'PONG':
                rtt = (time.time() - start) * 1000
                results[target['id']] = round(rtt, 2)
                print(f"[{node_id}] RTT to {target['id']} = {rtt:.2f} ms")
        except socket.timeout:
            results[target['id']] = 9999
    
    sock.close()
    
    msg = {"type": "PROBE_RESULTS", "node_id": node_id, "results": results}
    send_json(server_sock, msg)


def connect_to_neighbor(neighbor_id, neighbor_ip, neighbor_port):
    """Estabelece conexão TCP persistente com vizinho."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((neighbor_ip, neighbor_port))
        
        with lock:
            neighbor_connections[neighbor_id] = sock
            neighbors[neighbor_id] = {
                "ip": neighbor_ip,
                "port": neighbor_port,
                "conn": sock
            }
        
        print(f"[{node_id}] Connected to neighbor {neighbor_id} ({neighbor_ip}:{neighbor_port})")
        
        # Thread para escutar mensagens do vizinho
        threading.Thread(target=listen_neighbor, args=(neighbor_id, sock), daemon=True).start()
        
        return True
    except Exception as e:
        print(f"[{node_id}][ERROR] Failed to connect to {neighbor_id}: {e}")
        return False


def listen_neighbor(neighbor_id, sock):
    """Escuta mensagens de um vizinho overlay."""
    buffer = ""
    try:
        while True:
            data = sock.recv(4096)
            if not data:
                print(f"[{node_id}] Neighbor {neighbor_id} disconnected")
                break
            
            buffer += data.decode()
            
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                
                try:
                    msg = json.loads(line)
                    process_neighbor_message(msg, neighbor_id)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"[{node_id}][ERROR] Neighbor {neighbor_id} listener crashed: {e}")
    finally:
        with lock:
            if neighbor_id in neighbor_connections:
                del neighbor_connections[neighbor_id]
            if neighbor_id in neighbors:
                del neighbors[neighbor_id]


def process_neighbor_message(msg, from_neighbor):
    """Processa mensagens recebidas de vizinhos overlay."""
    msg_type = msg.get("type")
    
    if msg_type == "STREAM_ANNOUNCE":
        handle_stream_announce(msg, from_neighbor)
    
    elif msg_type == "STREAM_DATA":
        handle_stream_data(msg, from_neighbor)
    
    else:
        print(f"[{node_id}] Unknown message from {from_neighbor}: {msg_type}")


def handle_stream_announce(msg, from_neighbor):
    """Processa anúncio de stream (flooding)."""
    video_id = msg.get("video_id")
    source = msg.get("source")
    metric = msg.get("metric")
    path = msg.get("path", [])
    msg_id = msg.get("msg_id")
    video_name = msg.get("video_name")
    group = msg.get("group")
    port = msg.get("port")
    
    # Evitar loops
    if msg_id in seen_messages:
        return
    
    with lock:
        seen_messages.add(msg_id)
    
    # Verificar se já temos rota melhor
    with lock:
        if video_id in routes:
            if routes[video_id]["metric"] <= metric:
                return  # Rota atual é melhor ou igual
    
    # Atualizar rota
    with lock:
        routes[video_id] = {
            "source": source,
            "metric": metric,
            "prev_hop": from_neighbor,
            "next_hops": [],
            "active": False,
            "video_name": video_name,
            "group": group,
            "port": port
        }
    
    print(f"[{node_id}] Updated route for video {video_id}: metric={metric}, from={from_neighbor}")
    
    # Reencaminhar anúncio para outros vizinhos (exceto quem enviou)
    new_msg = msg.copy()
    new_msg["metric"] = metric + 1
    new_msg["path"] = path + [node_id]
    
    with lock:
        for neighbor_id, neighbor_info in neighbors.items():
            if neighbor_id == from_neighbor:
                continue  # Não enviar de volta
            
            if node_id in path:
                continue  # Evitar loops
            
            send_json(neighbor_info["conn"], new_msg)
            print(f"[{node_id}] Forwarded announce to {neighbor_id}")


def handle_stream_data(msg, from_neighbor):
    """Recebe dados de stream e reencaminha conforme tabela de rotas."""
    video_id = msg.get("video_id")
    data_chunk = msg.get("data")
    
    with lock:
        if video_id not in routes or not routes[video_id].get("active"):
            return  # Rota não ativa, descartar
        
        route = routes[video_id]
        next_hops = route["next_hops"]
    
    # Reencaminhar para próximos nós
    for next_hop in next_hops:
        with lock:
            if next_hop in neighbor_connections:
                send_json(neighbor_connections[next_hop], msg)


def start_relay(video_id, group, port, ingest_port):
    """
    Relay de vídeo: recebe UDP do nó anterior (ou servidor) e 
    retransmite via multicast local + encaminha para próximos nós overlay.
    """
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(('', ingest_port))
    print(f"[{node_id}][RELAY] Listening on UDP {ingest_port} for video {video_id}")
    
    # Socket multicast local
    mc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    ttl = struct.pack('b', 1)
    mc_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    
    print(f"[{node_id}][RELAY] Forwarding to multicast {group}:{port}")
    
    while True:
        try:
            data, _ = recv_sock.recvfrom(65507)
            if not data:
                continue
            
            # Enviar para multicast local (clientes)
            mc_sock.sendto(data, (group, port))
            
            # Reencaminhar para próximos nós overlay (se houver)
            with lock:
                if video_id in routes and routes[video_id].get("active"):
                    next_hops = routes[video_id]["next_hops"]
                    
                    for next_hop in next_hops:
                        if next_hop in neighbors:
                            neighbor = neighbors[next_hop]
                            # Enviar via UDP para o ingest_port do próximo nó
                            try:
                                relay_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                                relay_sock.sendto(data, (neighbor["ip"], INGEST_PORT))
                                relay_sock.close()
                            except Exception as e:
                                print(f"[{node_id}][RELAY][ERROR] Forward to {next_hop}: {e}")
        
        except Exception as e:
            print(f"[{node_id}][RELAY][ERROR] {e}")
            break
    
    recv_sock.close()
    mc_sock.close()


def keepalive(server_sock):
    """Envia keepalive periódico ao servidor."""
    while True:
        time.sleep(KEEPALIVE_INTERVAL)
        if node_id:
            msg = {"type": "KEEPALIVE", "node_id": node_id, "timestamp": time.time()}
            send_json(server_sock, msg)


def listen_server(server_sock):
    """Escuta mensagens do servidor (bootstrapper)."""
    buffer = ""
    try:
        while True:
            data = server_sock.recv(4096)
            if not data:
                print(f"[{node_id}] Server closed connection")
                break
            
            buffer += data.decode()
            
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if not line.strip():
                    continue
                
                try:
                    msg = json.loads(line)
                    process_server_message(msg, server_sock)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"[{node_id}][ERROR] Server listener crashed: {e}")


def process_server_message(msg, server_sock):
    """Processa mensagens do servidor."""
    global node_id
    msg_type = msg.get("type")
    
    if msg_type == "ASSIGN_ID":
        node_id = msg["node_id"]
        print(f"[INFO] Assigned node ID: {node_id}")
    
    elif msg_type == "NEIGHBORS":
        neighbor_list = msg.get("neighbors", [])
        print(f"[{node_id}] Received {len(neighbor_list)} neighbors")
        
        # Conectar a cada vizinho
        for neighbor in neighbor_list:
            nid = neighbor["id"]
            nip = neighbor["ip"]
            nport = neighbor["port"]
            
            if nid not in neighbor_connections:
                threading.Thread(
                    target=connect_to_neighbor,
                    args=(nid, nip, nport),
                    daemon=True
                ).start()
    
    elif msg_type in ("PROBE_TARGETS", "NEIGHBOR_UPDATE"):
        targets = msg.get("targets", [])
        print(f"[{node_id}] Probing {len(targets)} nodes...")
        threading.Thread(target=probe_nodes, args=(server_sock, targets), daemon=True).start()
    
    elif msg_type == "STREAM_ANNOUNCE":
        # Servidor enviou anúncio diretamente
        handle_stream_announce(msg, "SERVER")
    
    elif msg_type == "ACTIVATE_ROUTE":
        video_id = msg.get("video_id")
        next_hop = msg.get("next_hop")
        
        with lock:
            if video_id in routes:
                if next_hop not in routes[video_id]["next_hops"]:
                    routes[video_id]["next_hops"].append(next_hop)
                routes[video_id]["active"] = True
        
        print(f"[{node_id}] Activated route for video {video_id} -> {next_hop}")
    
    elif msg_type == "START_RELAY":
        video_id = msg.get("video_id")
        video_name = msg.get("video")
        group = msg.get("group")
        port = msg.get("port")
        ingest_port = msg.get("ingest_port", INGEST_PORT)
        
        print(f"[{node_id}][RELAY] Starting relay for video {video_id} ({video_name})")
        
        threading.Thread(
            target=start_relay,
            args=(video_id, group, port, ingest_port),
            daemon=True
        ).start()


def start_node():
    """Função principal do nó overlay."""
    global node_id
    
    # Conectar ao servidor
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.connect((SERVER_IP, SERVER_PORT))
        print(f"[INFO] Connected to bootstrapper {SERVER_IP}:{SERVER_PORT}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to server: {e}")
        return
    
    local_ip = get_ip_for_destination(SERVER_IP)
    print(f"[INFO] Local IP: {local_ip}")
    
    # Registar-se como nó overlay
    reg_msg = {
        "type": "REGISTER",
        "role": "overlay",
        "ip": local_ip,
        "port": UDP_PORT,
        "ingest_port": INGEST_PORT
    }
    send_json(server_sock, reg_msg)
    
    # Iniciar threads auxiliares
    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=keepalive, args=(server_sock,), daemon=True).start()
    threading.Thread(target=listen_server, args=(server_sock,), daemon=True).start()
    
    print(f"[INFO] Node started. Waiting for configuration...")
    
    # Manter processo vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Shutting down...")
        server_sock.close()


if __name__ == "__main__":
    start_node()