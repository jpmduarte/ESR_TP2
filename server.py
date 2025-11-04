import socket
import threading
import json
import time

HOST = '0.0.0.0'
PORT = 5000
KEEPALIVE_TIMEOUT = 10
MAX_NEIGHBORS = 3

nodes = {}           # { node_id: {ip, port, last_seen, conn, neighbors, active} }
node_counter = 1
probe_results = {}   # { node_id: {target_id: rtt_ms} }
lock = threading.Lock()


def send_json(conn, msg):
    """Send JSON message with newline delimiter."""
    try:
        conn.send((json.dumps(msg) + "\n").encode())
    except Exception as e:
        print(f"[ERROR] send_json: {e}")


def broadcast_to_active(msg_type, payload=None):
    with lock:
        for n_id, info in nodes.items():
            if not info.get("active", True):
                continue
            try:
                send_json(info["conn"], {"type": msg_type, **(payload or {})})
            except Exception as e:
                print(f"[WARN] Failed to send to {n_id}: {e}")


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
                process_message(msg, conn)
    except Exception as e:
        print(f"[ERROR] Client crashed: {e}")
    finally:
        conn.close()


def process_message(msg, conn):
    global node_counter
    msg_type = msg.get("type")

    # ====================== OVERLAY NODE REGISTRATION ======================
    if msg_type == "REGISTER":
        ip = msg.get("ip")
        port = msg.get("port")

        existing_id = None
        with lock:
            for n_id, info in nodes.items():
                if info["ip"] == ip and info["port"] == port:
                    existing_id = n_id
                    break

        if existing_id:
            node_id = existing_id
            with lock:
                nodes[node_id]["conn"] = conn
                nodes[node_id]["last_seen"] = time.time()
                nodes[node_id]["active"] = True
            print(f"[INFO] Node {node_id} reconnected ({ip}:{port}) — reactivated.")
        else:
            with lock:
                node_id = f"O{node_counter}"
                node_counter += 1
                nodes[node_id] = {
                    "ip": ip,
                    "port": port,
                    "last_seen": time.time(),
                    "conn": conn,
                    "neighbors": [],
                    "active": True
                }
            print(f"[INFO] New node {node_id} registered ({ip}:{port})")

        send_json(conn, {"type": "ASSIGN_ID", "node_id": node_id})
        threading.Thread(target=trigger_network_reprobe, daemon=True).start()

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
                {"id": n, "ip": nodes[n]["ip"], "port": nodes[n]["port"]}
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

    # ====================== CLIENT REGISTRATION ======================
    elif msg_type == "REGISTER_CLIENT":
        # Step 1: give the client the active OTT nodes to probe (RTT)
        client_ip = msg.get("ip")
        client_port = msg.get("port")
        print(f"[CLIENT] Register request from {client_ip}:{client_port}")

        with lock:
            active_nodes = {n: info for n, info in nodes.items() if info.get("active", True)}
            if not active_nodes:
                send_json(conn, {"type": "ERROR", "reason": "No active OTT nodes available"})
                print("[WARN] No active OTT nodes available for client.")
                return

            # Build probe target list for the client
            targets = [
                {"id": n, "ip": info["ip"], "port": info["port"]}
                for n, info in active_nodes.items()
            ]

        send_json(conn, {"type": "CLIENT_PROBE_TARGETS", "targets": targets})
        print(f"[CLIENT] Sent {len(targets)} probe targets to client {client_ip}:{client_port}")

    # ====================== CLIENT PROBE RESULTS ======================
    elif msg_type == "CLIENT_PROBE_RESULTS":
        # Step 2: client returns RTTs; choose node with minimal RTT
        results = msg.get("results", {})  # { "O2": 3.1, "O3": 8.7, ... }
        if not results:
            send_json(conn, {"type": "ERROR", "reason": "No probe results from client"})
            print("[CLIENT] Empty probe results from client.")
            return

        # Keep only current active nodes and pick minimum RTT
        with lock:
            active = {n for n, info in nodes.items() if info.get("active", True)}
        filtered = {nid: rtt for nid, rtt in results.items() if nid in active}

        if not filtered:
            send_json(conn, {"type": "ERROR", "reason": "No active OTT nodes matched results"})
            print("[CLIENT] No active nodes matched client's results.")
            return

        # Pick minimal RTT (ties broken arbitrarily)
        target_id = min(filtered.items(), key=lambda kv: kv[1])[0]
        with lock:
            target_info = nodes[target_id]

        send_json(conn, {
            "type": "ASSIGN_OTT",
            "ott": {"id": target_id, "ip": target_info["ip"], "port": target_info["port"]},
            "metric": {"rtt_ms": filtered[target_id]}
        })
        print(f"[CLIENT] Assigned client to {target_id} ({target_info['ip']}:{target_info['port']}), rtt={filtered[target_id]:.2f} ms")

    else:
        print(f"[WARN] Unknown message type {msg_type}")


def trigger_network_reprobe():
    """When new node joins or reactivates, tell all active nodes to re-probe."""
    time.sleep(1)
    with lock:
        active_nodes = {n: info for n, info in nodes.items() if info.get("active", True)}

    for n_id, info in active_nodes.items():
        targets = [
            {"id": nid, "ip": nfo["ip"], "port": nfo["port"]}
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
