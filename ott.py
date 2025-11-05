import socket
import json
import threading
import time
import subprocess
import re
import struct

SERVER_IP = '10.0.0.10'
SERVER_PORT = 5000
UDP_PORT = 6001         # usado para PING/PONG e neighbor probing
INGEST_PORT = 6500      # porta para ingestão de vídeo real
KEEPALIVE_INTERVAL = 5

node_id = None
neighbors = {}
routes = {}
lock = threading.Lock()


# -------------------------------------------------------------------
# Funções utilitárias
# -------------------------------------------------------------------
def send_json(sock, msg):
    sock.send((json.dumps(msg) + "\n").encode())


def get_ip_for_destination(dest_ip: str) -> str:
    """Descobre o IP local usado para comunicar com o destino (sem enviar pacotes)."""
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


# -------------------------------------------------------------------
# UDP listener — responde a PING/PONG
# -------------------------------------------------------------------
def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', UDP_PORT))
    print(f"[INFO] UDP listener active on port {UDP_PORT}")

    while True:
        data, addr = sock.recvfrom(1024)
        if data == b'PING':
            sock.sendto(b'PONG', addr)


# -------------------------------------------------------------------
# Relay de vídeo: recebe do servidor e retransmite via multicast
# -------------------------------------------------------------------
def start_relay(group, port, ingest_port):
    """Recebe chunks UDP do servidor e retransmite para o grupo multicast local."""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    recv_sock.bind(('', ingest_port))
    print(f"[RELAY] Listening for ingest on UDP {ingest_port}")

    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    ttl = struct.pack('b', 1)
    send_sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    print(f"[RELAY] Forwarding incoming stream to multicast {group}:{port}")

    while True:
        try:
            data, _ = recv_sock.recvfrom(65507)
            if not data:
                continue
            send_sock.sendto(data, (group, port))
        except Exception as e:
            print(f"[RELAY][ERROR] {e}")
            break

    recv_sock.close()
    send_sock.close()
    print("[RELAY] Relay stopped.")


# -------------------------------------------------------------------
# Probing entre nós overlay (RTT)
# -------------------------------------------------------------------
def probe_nodes(sock, targets):
    results = {}
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.settimeout(1.0)

    for t in targets:
        start = time.time()
        try:
            udp_sock.sendto(b'PING', (t['ip'], UDP_PORT))
            data, _ = udp_sock.recvfrom(1024)
            if data == b'PONG':
                rtt = (time.time() - start) * 1000
                results[t['id']] = rtt
                print(f"[{node_id}] RTT to {t['ip']} ({t['id']}) = {rtt:.2f} ms")
        except socket.timeout:
            results[t['id']] = 9999

    udp_sock.close()
    msg = {"type": "PROBE_RESULTS", "node_id": node_id, "results": results}
    send_json(sock, msg)


# -------------------------------------------------------------------
# Keepalive para o servidor
# -------------------------------------------------------------------
def keepalive(sock):
    while True:
        time.sleep(KEEPALIVE_INTERVAL)
        if node_id:
            msg = {"type": "KEEPALIVE", "node_id": node_id, "timestamp": time.time()}
            send_json(sock, msg)


# -------------------------------------------------------------------
# Tabela de rotas overlay (mantém original)
# -------------------------------------------------------------------
def recompute_routes():
    global routes
    with lock:
        new_routes = {}
        for n_id, n in neighbors.items():
            new_routes[n["ip"]] = {"next_hop": n["ip"], "port": n["port"]}
        routes = new_routes
    print(f"[{node_id}] Routing table updated:")
    for ip, r in routes.items():
        print(f"   → {ip} via {r['next_hop']}:{r['port']}")


# -------------------------------------------------------------------
# Nó principal
# -------------------------------------------------------------------
def start_node():
    global node_id, neighbors

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((SERVER_IP, SERVER_PORT))
    print(f"[INFO] Connected to server {SERVER_IP}:{SERVER_PORT}")

    local_ip = get_ip_for_destination(SERVER_IP)
    print(f"[INFO] Local IP detected: {local_ip}")

    reg = {
        "type": "REGISTER",
        "role": "overlay",
        "ip": local_ip,
        "port": UDP_PORT,
        "ingest_port": INGEST_PORT
    }
    send_json(sock, reg)

    threading.Thread(target=keepalive, args=(sock,), daemon=True).start()
    threading.Thread(target=udp_listener, daemon=True).start()

    buffer = ""
    while True:
        data = sock.recv(4096)
        if not data:
            print("[WARN] Server closed connection.")
            break
        buffer += data.decode()

        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if not line.strip():
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            t = msg.get("type")

            if t == "ASSIGN_ID":
                node_id = msg["node_id"]
                print(f"[INFO] Assigned node ID {node_id}")

            elif t in ("PROBE_TARGETS", "NEIGHBOR_UPDATE"):
                targets = msg.get("targets", [])
                print(f"[{node_id}] Re-probing {len(targets)} nodes after topology update...")
                threading.Thread(target=probe_nodes, args=(sock, targets), daemon=True).start()

            elif t == "NEIGHBORS":
                new_neighbors = msg["neighbors"]
                with lock:
                    neighbors = {n["id"]: {"ip": n["ip"], "port": n["port"], "rtt": 0.0} for n in new_neighbors}
                print(f"[{node_id}] Neighbor list:")
                for n_id, info in neighbors.items():
                    print(f"   → {info['ip']}:{info['port']}")
                recompute_routes()

            elif t == "START_RELAY":
                video = msg.get("video")
                group = msg.get("group")
                port = msg.get("port")
                ingest_port = msg.get("ingest_port", INGEST_PORT)
                print(f"[RELAY] Starting relay for {video} ({group}:{port}) ingest={ingest_port}")
                threading.Thread(target=start_relay, args=(group, port, ingest_port), daemon=True).start()

    sock.close()
    print("[INFO] Node shut down.")


if __name__ == "__main__":
    start_node()
