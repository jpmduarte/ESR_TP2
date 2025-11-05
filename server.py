import socket
import threading
import json
import time
import os
import struct

HOST = '0.0.0.0'
PORT = 5000

KEEPALIVE_TIMEOUT = 10
MAX_NEIGHBORS = 3

# Streaming
VIDEO_DIR = "videos"
MC_GROUP_BASE = "239.0.0."
MC_PORT_BASE = 5001
UDP_CHUNK = 65507         # máx payload UDP
FRAME_DELAY = 0.04        # ~25 fps
DEFAULT_INGEST_PORT = 6500

# Estado
nodes = {}           # { node_id: {ip, probe_port, ingest_port, last_seen, conn, neighbors, active} }
node_counter = 1
probe_results = {}   # { node_id: {target_id: rtt_ms} }
lock = threading.Lock()

# cliente -> melhor nó escolhido no último probe (memória simples opcional)
client_last_pick = {}  # {(client_ip, client_tcp_port): node_id}


def send_json(conn, msg):
    """Send JSON message with newline delimiter."""
    try:
        conn.send((json.dumps(msg) + "\n").encode())
    except Exception as e:
        print(f"[ERROR] send_json: {e}")


def handle_client(conn, addr):
    print(f"[INFO] Connection from {addr}")
    buffer = ""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
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
                process_message(msg, conn, addr)
    except Exception as e:
        print(f"[ERROR] Client crashed: {e}")
    finally:
        conn.close()


# ---------------------------- util de streams ----------------------------

def enumerate_streams():
    """Lista vídeos disponíveis com mapeamento para grupo multicast e porto."""
    vids = []
    idx = 0
    for name in sorted(os.listdir(VIDEO_DIR)):
        if not name.lower().endswith((".mp4", ".mov", ".mkv", ".avi")):
            continue
        idx += 1
        vids.append({
            "id": idx,
            "name": name,
            "group": f"{MC_GROUP_BASE}{idx}",
            "port": MC_PORT_BASE + idx
        })
    return vids


def stream_file_to_node(video_path, node_ip, ingest_port):
    """Envia o ficheiro por UDP unicast para o OTT (porto de ingestão)."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(UDP_CHUNK)
                if not chunk:
                    break
                sock.sendto(chunk, (node_ip, ingest_port))
                time.sleep(FRAME_DELAY)
        sock.close()
        print(f"[STREAM] Finished sending {os.path.basename(video_path)} to {node_ip}:{ingest_port}")
    except Exception as e:
        print(f"[STREAM][ERROR] {e}")


# ---------------------------- protocolo ----------------------------

def process_message(msg, conn, peer_addr):
    global node_counter
    msg_type = msg.get("type")

    # ====================== OVERLAY NODE REGISTRATION ======================
    if msg_type == "REGISTER":
        ip = msg.get("ip")
        probe_port = msg.get("port")                     # porta onde o nó responde a PING/PONG
        ingest_port = msg.get("ingest_port", DEFAULT_INGEST_PORT)  # nova: porto de ingestão
        existing_id = None
        with lock:
            for n_id, info in nodes.items():
                if info["ip"] == ip and info["probe_port"] == probe_port:
                    existing_id = n_id
                    break

        if existing_id:
            node_id = existing_id
            with lock:
                nodes[node_id]["conn"] = conn
                nodes[node_id]["last_seen"] = time.time()
                nodes[node_id]["active"] = True
                nodes[node_id]["ingest_port"] = ingest_port
            print(f"[INFO] Node {node_id} reconnected ({ip}:{probe_port}) — reactivated.")
        else:
            with lock:
                node_id = f"O{node_counter}"
                node_counter += 1
                nodes[node_id] = {
                    "ip": ip,
                    "probe_port": probe_port,
                    "ingest_port": ingest_port,
                    "last_seen": time.time(),
                    "conn": conn,
                    "neighbors": [],
                    "active": True
                }
            print(f"[INFO] New node {node_id} registered ({ip}:{probe_port}, ingest:{ingest_port})")

        send_json(conn, {"type": "ASSIGN_ID", "node_id": node_id})
        # Topologia (continua a existir para overlay, se precisares)
        threading.Thread(target=trigger_network_reprobe, daemon=True).start()

        # Anunciar streams disponíveis a cada registo/conexão
        send_json(conn, {"type": "VIDEO_ANNOUNCE", "videos": enumerate_streams()})

    # ====================== OVERLAY NODE PROBE RESULTS ======================
    elif msg_type == "PROBE_RESULTS":
        node_id = msg["node_id"]
        results = msg["results"]
        print(f"[PROBE_RESULTS] from {node_id}: {results}")

        with lock:
            probe_results[node_id] = results
            sorted_nodes = sorted(results.items(), key=lambda x: x[1])
            best = [n for n, _ in sorted_nodes[:MAX_NEIGHBORS]]
            if node_id in nodes:
                nodes[node_id]["neighbors"] = best

            for n in best:
                if n in nodes and node_id not in nodes[n]["neighbors"]:
                    nodes[n]["neighbors"].append(node_id)

            neighbors_info = [
                {"id": n, "ip": nodes[n]["ip"], "port": nodes[n]["probe_port"]}
                for n in best if n in nodes and nodes[n].get("active", True)
            ]
        send_json(conn, {"type": "NEIGHBORS", "neighbors": neighbors_info})

    # ====================== KEEPALIVE ======================
    elif msg_type == "KEEPALIVE":
        node_id = msg.get("node_id")
        with lock:
            if node_id in nodes:
                nodes[node_id]["last_seen"] = time.time()
                nodes[node_id]["active"] = True

    # ====================== CLIENT: LISTA DE STREAMS ======================
    elif msg_type == "LIST_STREAMS":
        streams = enumerate_streams()
        send_json(conn, {"type": "STREAM_LIST", "videos": streams})

    # ====================== CLIENT: PROBING DE NÓS ======================
    elif msg_type == "REGISTER_CLIENT":
        client_ip = msg.get("ip")
        client_port = msg.get("port")
        print(f"[CLIENT] Register request from {client_ip}:{client_port}")

        with lock:
            active_nodes = {n: info for n, info in nodes.items() if info.get("active", True)}
            if not active_nodes:
                send_json(conn, {"type": "ERROR", "reason": "No active OTT nodes available"})
                print("[WARN] No active OTT nodes available for client.")
                return

            targets = [
                {"id": n, "ip": info["ip"], "port": info["probe_port"]}
                for n, info in active_nodes.items()
            ]

        send_json(conn, {"type": "CLIENT_PROBE_TARGETS", "targets": targets})
        print(f"[CLIENT] Sent {len(targets)} probe targets to client {client_ip}:{client_port}")

    elif msg_type == "CLIENT_PROBE_RESULTS":
        results = msg.get("results", {})
        if not results:
            send_json(conn, {"type": "ERROR", "reason": "No probe results from client"})
            print("[CLIENT] Empty probe results from client.")
            return

        with lock:
            active = {n for n, info in nodes.items() if info.get("active", True)}
        filtered = {nid: rtt for nid, rtt in results.items() if nid in active}

        if not filtered:
            send_json(conn, {"type": "ERROR", "reason": "No active OTT nodes matched results"})
            print("[CLIENT] No active nodes matched client's results.")
            return

        target_id = min(filtered.items(), key=lambda kv: kv[1])[0]
        with lock:
            target_info = nodes[target_id]

        # memoriza escolha (opcional)
        client_last_pick[peer_addr] = target_id

        send_json(conn, {
            "type": "ASSIGN_OTT",
            "ott": {"id": target_id, "ip": target_info["ip"], "port": target_info["probe_port"]},
            "metric": {"rtt_ms": filtered[target_id]}
        })
        print(f"[CLIENT] Assigned client to {target_id} ({target_info['ip']}:{target_info['probe_port']}), rtt={filtered[target_id]:.2f} ms")

    # ====================== CLIENT: PEDIDO DE STREAM ======================
    elif msg_type == "REQUEST_STREAM":
        """
        Espera: { "video_id": int, "ott_id": "O2" }
        - O servidor arranca o envio do ficheiro por UDP para o ingest_port do OTT.
        - Envia ao OTT um START_RELAY com (video, group, port) para o multicast local.
        - Devolve ao cliente os metadados do multicast (group/port) para ele juntar-se.
        """
        video_id = int(msg.get("video_id", 0))
        ott_id = msg.get("ott_id")
        videos = enumerate_streams()
        video_map = {v["id"]: v for v in videos}
        if video_id not in video_map:
            send_json(conn, {"type": "ERROR", "reason": "Invalid video_id"})
            return

        if not ott_id or ott_id not in nodes or not nodes[ott_id].get("active", True):
            send_json(conn, {"type": "ERROR", "reason": "Invalid or inactive ott_id"})
            return

        v = video_map[video_id]
        node = nodes[ott_id]
        video_path = os.path.join(VIDEO_DIR, v["name"])

        # 1) pedir ao OTT para iniciar multicast local (relay)
        start_msg = {
            "type": "START_RELAY",
            "video": v["name"],
            "group": v["group"],
            "port": v["port"],
            "ingest_port": node.get("ingest_port", DEFAULT_INGEST_PORT)
        }
        send_json(node["conn"], start_msg)
        print(f"[RELAY] Instructed {ott_id} to relay {v['name']} on {v['group']}:{v['port']} (ingest:{start_msg['ingest_port']})")

        # 2) arranca thread a enviar o vídeo real por UDP para o OTT (ingest)
        threading.Thread(
            target=stream_file_to_node,
            args=(video_path, node["ip"], node.get("ingest_port", DEFAULT_INGEST_PORT)),
            daemon=True
        ).start()

        # 3) responde ao cliente com info do multicast
        send_json(conn, {
            "type": "STREAM_READY",
            "video": v["name"],
            "group": v["group"],
            "port": v["port"],
            "ott_id": ott_id
        })

    else:
        print(f"[WARN] Unknown message type {msg_type}")


# ---------------------------- topologia overlay (continua igual) ----------------------------

def trigger_network_reprobe():
    """When new node joins or reactivates, tell all active nodes to re-probe."""
    time.sleep(1)
    with lock:
        active_nodes = {n: info for n, info in nodes.items() if info.get("active", True)}

    for n_id, info in active_nodes.items():
        targets = [
            {"id": nid, "ip": nfo["ip"], "port": nfo["probe_port"]}
            for nid, nfo in active_nodes.items() if nid != n_id
        ]
        if targets:
            send_json(info["conn"], {"type": "NEIGHBOR_UPDATE", "targets": targets})
            print(f"[TOPOLOGY] Requested NEIGHBOR_UPDATE from {n_id} ({len(targets)} targets)")


def monitor_nodes():
    while True:
        time.sleep(2)
        now = time.time()
        with lock:
            for n, info in list(nodes.items()):
                if info.get("active", True) and now - info["last_seen"] > KEEPALIVE_TIMEOUT:
                    info["active"] = False
                    print(f"[WARN] Node {n} timed out — inactive.")


def start_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(10)
    print(f"[BOOTSTRAPPER] Listening on {HOST}:{PORT}")

    threading.Thread(target=monitor_nodes, daemon=True).start()
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    start_server()
