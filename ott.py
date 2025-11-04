import socket
import json
import threading
import time
import subprocess
import re

SERVER_IP = '10.0.0.10'
SERVER_PORT = 5000
UDP_PORT = 6001
KEEPALIVE_INTERVAL = 5

node_id = None
neighbors = {}       # {id: {"ip": str, "port": int, "rtt": float}}
routes = {}          # {dest_ip: {"next_hop": str, "port": int}}
clients = set()      # {(client_ip, client_port)}
lock = threading.Lock()


# ----------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------
def send_json(sock, msg):
    sock.send((json.dumps(msg) + "\n").encode())


def get_ip_for_destination(dest_ip: str) -> str:
    """Discover local IP used to reach the given destination (no traffic sent)."""
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


# ----------------------------------------------------------
# UDP listener — handles both probe PINGs and client JOIN_STREAM
# ----------------------------------------------------------
def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', UDP_PORT))
    print(f"[INFO] UDP listener on port {UDP_PORT}")

    while True:
        data, addr = sock.recvfrom(1024)

        if data == b'PING':
            sock.sendto(b'PONG', addr)

        elif data == b'JOIN_STREAM':
            with lock:
                clients.add(addr)
            print(f"[STREAM] Client {addr} joined stream.")
            # Start stream thread if not already running
            if not any(t.name == "stream_thread" for t in threading.enumerate()):
                threading.Thread(target=send_stream, name="stream_thread", daemon=True).start()

        else:
            pass  # Ignore unknown data


# ----------------------------------------------------------
# Periodic UDP stream sender
# ----------------------------------------------------------
def send_stream():
    """Simulate video streaming by sending UDP packets to all connected clients."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    frame = 0
    print("[STREAM] Stream thread started.")
    while True:
        with lock:
            targets = list(clients)
        frame += 1
        payload = f"FRAME {frame}".encode()
        for addr in targets:
            sock.sendto(payload, addr)
        time.sleep(0.5)  # ~2 frames/second for demo


# ----------------------------------------------------------
# Probe other overlay nodes
# ----------------------------------------------------------
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


# ----------------------------------------------------------
# Keepalive loop
# ----------------------------------------------------------
def keepalive(sock):
    while True:
        time.sleep(KEEPALIVE_INTERVAL)
        if node_id:
            msg = {"type": "KEEPALIVE", "node_id": node_id, "timestamp": time.time()}
            send_json(sock, msg)


# ----------------------------------------------------------
# Build routing table from neighbors
# ----------------------------------------------------------
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


# ----------------------------------------------------------
# Main control loop (TCP)
# ----------------------------------------------------------
def start_node():
    global node_id, neighbors

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((SERVER_IP, SERVER_PORT))
    print(f"[INFO] Connected to server {SERVER_IP}:{SERVER_PORT}")

    local_ip = get_ip_for_destination(SERVER_IP)
    print(f"[INFO] Local IP detected: {local_ip}")

    reg = {"type": "REGISTER", "role": "overlay", "ip": local_ip, "port": UDP_PORT}
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

    sock.close()
    print("[INFO] Node shut down.")


if __name__ == "__main__":
    start_node()
