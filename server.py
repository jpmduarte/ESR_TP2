import socket
import threading
import json
import time
import os
import struct

HOST = '0.0.0.0'
PORT = 5000

KEEPALIVE_TIMEOUT = 15
MAX_NEIGHBORS = 3

# Streaming
VIDEO_DIR = "videos"
MC_GROUP_BASE = "239.0.0."
MC_PORT_BASE = 5001
UDP_CHUNK = 65507
FRAME_DELAY = 0.04
DEFAULT_INGEST_PORT = 6500

# Estado global
nodes = {}  # {node_id: {ip, probe_port, ingest_port, last_seen, conn, neighbors, active, role}}
node_counter = 1
probe_results = {}  # {node_id: {target_id: rtt_ms}}
lock = threading.Lock()

# Rotas ativas por vídeo: {video_id: {node_id: [next_hops]}}
active_routes = {}

# Streams em progresso: {video_id: {active: bool, clients: [ott_ids]}}
active_streams = {}


def send_json(conn, msg):
    """Envia mensagem JSON com delimitador de linha."""
    try:
        conn.send((json.dumps(msg) + "\n").encode())
        return True
    except Exception as e:
        print(f"[ERROR] send_json failed: {e}")
        return False


def enumerate_streams():
    """Lista vídeos disponíveis com mapeamento multicast."""
    vids = []
    if not os.path.exists(VIDEO_DIR):
        os.makedirs(VIDEO_DIR)
        print(f"[WARN] Created empty {VIDEO_DIR} directory")
        return vids
    
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


def stream_file_to_node(video_path, node_ip, ingest_port, video_id):
    """Envia ficheiro via UDP para o nó overlay (servidor da stream)."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print(f"[STREAM] Starting transmission of {os.path.basename(video_path)} to {node_ip}:{ingest_port}")
        
        with open(video_path, "rb") as f:
            packet_count = 0
            while video_id in active_streams and active_streams[video_id].get("active", False):
                chunk = f.read(UDP_CHUNK)
                if not chunk:
                    # Loop do vídeo
                    f.seek(0)
                    continue
                
                sock.sendto(chunk, (node_ip, ingest_port))
                packet_count += 1
                if packet_count % 100 == 0:
                    print(f"[STREAM] Sent {packet_count} packets for video {video_id}")
                time.sleep(FRAME_DELAY)
        
        sock.close()
        print(f"[STREAM] Stopped transmission of video {video_id}")
    except Exception as e:
        print(f"[STREAM][ERROR] {e}")


def assign_neighbors(new_node_id):
    """Atribui até MAX_NEIGHBORS vizinhos ao novo nó overlay."""
    with lock:
        # Apenas nós overlay ativos (excluindo clientes)
        candidates = [
            nid for nid, info in nodes.items() 
            if nid != new_node_id 
            and info.get("active", False) 
            and info.get("role") == "overlay"
        ]
        
        if not candidates:
            return []
        
        # Estratégia: selecionar nós com menos vizinhos (balanceamento)
        candidates.sort(key=lambda n: len(nodes[n].get("neighbors", [])))
        
        selected = candidates[:MAX_NEIGHBORS]
        
        # Atualizar bidireccionalmente
        for neighbor_id in selected:
            if new_node_id not in nodes[neighbor_id]["neighbors"]:
                nodes[neighbor_id]["neighbors"].append(new_node_id)
        
        nodes[new_node_id]["neighbors"] = selected
        
        print(f"[TOPOLOGY] Assigned neighbors to {new_node_id}: {selected}")
        return selected


def broadcast_stream_announce(video_id):
    """Anuncia stream disponível a todos os nós overlay (flooding)."""
    video_map = {v["id"]: v for v in enumerate_streams()}
    if video_id not in video_map:
        return
    
    video = video_map[video_id]
    
    msg = {
        "type": "STREAM_ANNOUNCE",
        "video_id": video_id,
        "video_name": video["name"],
        "group": video["group"],
        "port": video["port"],
        "source": "O1",  # Servidor é sempre O1
        "metric": 0,
        "path": ["O1"],
        "msg_id": f"{video_id}-{time.time()}"  # Evitar loops
    }
    
    with lock:
        overlay_nodes = {nid: info for nid, info in nodes.items() 
                        if info.get("role") == "overlay" and info.get("active")}
    
    print(f"[ANNOUNCE] Broadcasting stream {video_id} to {len(overlay_nodes)} overlay nodes")
    
    for node_id, info in overlay_nodes.items():
        if send_json(info["conn"], msg):
            print(f"[ANNOUNCE] Sent to {node_id}")


def activate_stream_path(video_id, client_ott_id):
    """Ativa caminho desde servidor (O1) até ao OTT do cliente."""
    # Procurar caminho usando BFS na topologia overlay
    path = find_path_bfs("O1", client_ott_id)
    
    if not path:
        print(f"[ERROR] No path found from O1 to {client_ott_id}")
        return False
    
    print(f"[ACTIVATE] Path for video {video_id}: {' -> '.join(path)}")
    
    # Enviar mensagens de ativação ao longo do caminho
    for i in range(len(path) - 1):
        current = path[i]
        next_hop = path[i + 1]
        
        if current not in nodes:
            continue
        
        msg = {
            "type": "ACTIVATE_ROUTE",
            "video_id": video_id,
            "next_hop": next_hop
        }
        send_json(nodes[current]["conn"], msg)
        print(f"[ACTIVATE] {current} -> {next_hop}")
    
    # Registar rota ativa
    if video_id not in active_routes:
        active_routes[video_id] = {}
    
    for i in range(len(path) - 1):
        node = path[i]
        if node not in active_routes[video_id]:
            active_routes[video_id][node] = []
        if path[i + 1] not in active_routes[video_id][node]:
            active_routes[video_id][node].append(path[i + 1])
    
    return True


def find_path_bfs(source, destination):
    """Encontra caminho mais curto entre dois nós usando BFS."""
    if source == destination:
        return [source]
    
    with lock:
        if source not in nodes or destination not in nodes:
            return None
        
        visited = {source}
        queue = [(source, [source])]
        
        while queue:
            current, path = queue.pop(0)
            
            for neighbor in nodes[current].get("neighbors", []):
                if neighbor in visited:
                    continue
                
                visited.add(neighbor)
                new_path = path + [neighbor]
                
                if neighbor == destination:
                    return new_path
                
                queue.append((neighbor, new_path))
    
    return None


def trigger_network_reprobe():
    """Quando há mudança na topologia, pedir re-probing."""
    time.sleep(1)
    
    with lock:
        active_nodes = {n: info for n, info in nodes.items() 
                       if info.get("active") and info.get("role") == "overlay"}
    
    for node_id, info in active_nodes.items():
        targets = [
            {"id": nid, "ip": nfo["ip"], "port": nfo["probe_port"]}
            for nid, nfo in active_nodes.items() if nid != node_id
        ]
        
        if targets:
            msg = {"type": "NEIGHBOR_UPDATE", "targets": targets}
            send_json(info["conn"], msg)
            print(f"[TOPOLOGY] Sent reprobe request to {node_id} ({len(targets)} targets)")


def process_message(msg, conn, peer_addr):
    global node_counter
    msg_type = msg.get("type")

    # ==================== REGISTRO DE NÓ OVERLAY ====================
    if msg_type == "REGISTER":
        ip = msg.get("ip")
        probe_port = msg.get("port")
        ingest_port = msg.get("ingest_port", DEFAULT_INGEST_PORT)
        role = msg.get("role", "overlay")
        
        existing_id = None
        with lock:
            for nid, info in nodes.items():
                if info["ip"] == ip and info["probe_port"] == probe_port:
                    existing_id = nid
                    break
        
        if existing_id:
            node_id = existing_id
            with lock:
                nodes[node_id]["conn"] = conn
                nodes[node_id]["last_seen"] = time.time()
                nodes[node_id]["active"] = True
                nodes[node_id]["ingest_port"] = ingest_port
                nodes[node_id]["role"] = role
            print(f"[INFO] Node {node_id} reconnected ({ip}:{probe_port})")
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
                    "active": True,
                    "role": role
                }
            print(f"[INFO] New {role} node {node_id} registered ({ip}:{probe_port})")
        
        send_json(conn, {"type": "ASSIGN_ID", "node_id": node_id})
        
        if role == "overlay":
            # Atribuir vizinhos
            neighbors = assign_neighbors(node_id)
            neighbors_info = [
                {"id": n, "ip": nodes[n]["ip"], "port": nodes[n]["probe_port"]}
                for n in neighbors if n in nodes
            ]
            send_json(conn, {"type": "NEIGHBORS", "neighbors": neighbors_info})
            
            # Trigger reprobe
            threading.Thread(target=trigger_network_reprobe, daemon=True).start()
            
            # Anunciar streams existentes
            for video in enumerate_streams():
                broadcast_stream_announce(video["id"])

    # ==================== RESULTADOS DE PROBING ====================
    elif msg_type == "PROBE_RESULTS":
        node_id = msg["node_id"]
        results = msg["results"]
        print(f"[PROBE] Results from {node_id}: {results}")
        
        with lock:
            probe_results[node_id] = results

    # ==================== KEEPALIVE ====================
    elif msg_type == "KEEPALIVE":
        node_id = msg.get("node_id")
        with lock:
            if node_id in nodes:
                nodes[node_id]["last_seen"] = time.time()
                nodes[node_id]["active"] = True

    # ==================== CLIENT: REGISTRO ====================
    elif msg_type == "REGISTER_CLIENT":
        client_ip = msg.get("ip")
        client_port = msg.get("port")
        print(f"[CLIENT] Registration from {client_ip}:{client_port}")
        
        with lock:
            active_overlay = {n: info for n, info in nodes.items() 
                            if info.get("active") and info.get("role") == "overlay"}
        
        if not active_overlay:
            send_json(conn, {"type": "ERROR", "reason": "No active OTT nodes"})
            return
        
        targets = [
            {"id": n, "ip": info["ip"], "port": info["probe_port"]}
            for n, info in active_overlay.items()
        ]
        
        send_json(conn, {"type": "CLIENT_PROBE_TARGETS", "targets": targets})
        print(f"[CLIENT] Sent {len(targets)} probe targets")

    # ==================== CLIENT: RESULTADOS DE PROBING ====================
    elif msg_type == "CLIENT_PROBE_RESULTS":
        results = msg.get("results", {})
        
        if not results:
            send_json(conn, {"type": "ERROR", "reason": "Empty probe results"})
            return
        
        with lock:
            active = {n for n, info in nodes.items() 
                     if info.get("active") and info.get("role") == "overlay"}
        
        filtered = {nid: rtt for nid, rtt in results.items() if nid in active}
        
        if not filtered:
            send_json(conn, {"type": "ERROR", "reason": "No active nodes matched"})
            return
        
        best_ott = min(filtered.items(), key=lambda x: x[1])[0]
        
        with lock:
            ott_info = nodes[best_ott]
        
        send_json(conn, {
            "type": "ASSIGN_OTT",
            "ott": {
                "id": best_ott,
                "ip": ott_info["ip"],
                "port": ott_info["probe_port"]
            },
            "metric": {"rtt_ms": filtered[best_ott]}
        })
        print(f"[CLIENT] Assigned to {best_ott} (RTT: {filtered[best_ott]:.2f} ms)")

    # ==================== CLIENT: LISTA DE STREAMS ====================
    elif msg_type == "LIST_STREAMS":
        streams = enumerate_streams()
        send_json(conn, {"type": "STREAM_LIST", "videos": streams})
        print(f"[CLIENT] Sent {len(streams)} available streams")

    # ==================== CLIENT: REQUEST STREAM ====================
    elif msg_type == "REQUEST_STREAM":
        video_id = msg.get("video_id")
        client_ott_id = msg.get("ott_id")
        
        videos = enumerate_streams()
        video_map = {v["id"]: v for v in videos}
        
        if video_id not in video_map:
            send_json(conn, {"type": "ERROR", "reason": "Invalid video_id"})
            return
        
        if not client_ott_id or client_ott_id not in nodes:
            send_json(conn, {"type": "ERROR", "reason": "Invalid ott_id"})
            return
        
        video = video_map[video_id]
        video_path = os.path.join(VIDEO_DIR, video["name"])
        
        if not os.path.exists(video_path):
            send_json(conn, {"type": "ERROR", "reason": "Video file not found"})
            return
        
        print(f"[STREAM] Request for video {video_id} from client at OTT {client_ott_id}")
        
        # Ativar rota no overlay
        if activate_stream_path(video_id, client_ott_id):
            # Instruir nó do cliente a fazer relay local
            client_node = nodes[client_ott_id]
            relay_msg = {
                "type": "START_RELAY",
                "video_id": video_id,
                "video": video["name"],
                "group": video["group"],
                "port": video["port"],
                "ingest_port": client_node.get("ingest_port", DEFAULT_INGEST_PORT)
            }
            send_json(client_node["conn"], relay_msg)
            
            # Responder ao cliente com info do multicast
            send_json(conn, {
                "type": "STREAM_READY",
                "video": video["name"],
                "group": video["group"],
                "port": video["port"],
                "ott_id": client_ott_id
            })
            
            # Iniciar streaming se ainda não ativo
            if video_id not in active_streams or not active_streams[video_id].get("active"):
                active_streams[video_id] = {"active": True, "clients": []}
                
                # Stream vai para O1 (assumindo que servidor é O1)
                # Em produção, o servidor seria um nó overlay separado
                server_node = nodes.get("O1")
                if server_node:
                    threading.Thread(
                        target=stream_file_to_node,
                        args=(video_path, server_node["ip"], 
                              server_node.get("ingest_port", DEFAULT_INGEST_PORT), video_id),
                        daemon=True
                    ).start()
            
            active_streams[video_id]["clients"].append(client_ott_id)
            print(f"[STREAM] Stream {video_id} activated for {client_ott_id}")
        else:
            send_json(conn, {"type": "ERROR", "reason": "Failed to activate path"})

    else:
        print(f"[WARN] Unknown message type: {msg_type}")


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
                    process_message(msg, conn, addr)
                except json.JSONDecodeError as e:
                    print(f"[ERROR] JSON decode error: {e}")
    except Exception as e:
        print(f"[ERROR] Client handler crashed: {e}")
    finally:
        conn.close()


def monitor_nodes():
    """Monitor keepalives e marca nós inativos."""
    while True:
        time.sleep(3)
        now = time.time()
        
        with lock:
            for node_id, info in list(nodes.items()):
                if info.get("active") and now - info["last_seen"] > KEEPALIVE_TIMEOUT:
                    info["active"] = False
                    print(f"[WARN] Node {node_id} timed out (inactive)")


def start_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen(20)
    print(f"[BOOTSTRAPPER] Server listening on {HOST}:{PORT}")
    print(f"[INFO] Video directory: {VIDEO_DIR}")
    print(f"[INFO] Available videos: {len(enumerate_streams())}")
    
    threading.Thread(target=monitor_nodes, daemon=True).start()
    
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    start_server()