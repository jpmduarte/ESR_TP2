import socket
import json
import time
import threading
import random

SERVER_IP = "10.0.0.10"
SERVER_PORT = 5000
UDP_PORT = random.randint(7000, 8000)
node_targets = []


def send_json(sock, msg):
    sock.send((json.dumps(msg) + "\n").encode())


def udp_probe(targets):
    """Measure RTT to each OTT node via UDP PING/PONG using the SAME socket/port."""
    results = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", UDP_PORT))         # <- fixa a porta de origem
    sock.settimeout(1.0)

    for t in targets:
        addr = (t["ip"], t["port"])
        start = time.time()
        try:
            sock.sendto(b"PING", addr)          # envia a partir da UDP_PORT
            data, _ = sock.recvfrom(1024)       # recebe na MESMA porta
            if data == b"PONG":
                rtt = (time.time() - start) * 1000
                results[t["id"]] = round(rtt, 2)
                print(f"[CLIENT] RTT to {t['id']} ({t['ip']}) = {rtt:.2f} ms")
        except socket.timeout:
            results[t["id"]] = 9999

    sock.close()
    return results


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((SERVER_IP, SERVER_PORT))
    local_ip = sock.getsockname()[0]
    print(f"[CLIENT] Connected to server {SERVER_IP}:{SERVER_PORT}")

    # Step 1 — Register client
    reg = {"type": "REGISTER_CLIENT", "ip": local_ip, "port": UDP_PORT}
    send_json(sock, reg)

    buffer = ""
    assigned_ott = None

    while True:
        data = sock.recv(4096)
        if not data:
            break
        buffer += data.decode()

        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            if not line.strip():
                continue

            msg = json.loads(line)
            msg_type = msg.get("type")

            # Step 2 — Server sent OTTs to probe
            if msg_type == "CLIENT_PROBE_TARGETS":
                targets = msg.get("targets", [])
                if not targets:
                    print("[ERROR] No targets to probe.")
                    return

                print(f"[CLIENT] Received {len(targets)} probe targets.")
                results = udp_probe(targets)

                # Step 3 — send results back to server
                send_json(sock, {"type": "CLIENT_PROBE_RESULTS", "results": results})

            # Step 4 — Server assigns best OTT
            elif msg_type == "ASSIGN_OTT":
                assigned_ott = msg["ott"]
                metric = msg.get("metric", {})
                print(f"[CLIENT] Assigned OTT: {assigned_ott} (RTT={metric.get('rtt_ms', '?')} ms)")

                # Start listening for data from OTT
                threading.Thread(target=listen_stream, args=(assigned_ott,), daemon=True).start()

                # Request stream
                send_join_stream(assigned_ott)
                return

            elif msg_type == "ERROR":
                print(f"[ERROR] Server error: {msg}")
                return


def send_join_stream(ott):
    """Send JOIN_STREAM to OTT via UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    msg = b"JOIN_STREAM"
    sock.sendto(msg, (ott["ip"], ott["port"]))
    print(f"[CLIENT] Requested stream from OTT {ott['ip']}:{ott['port']}")
    sock.close()


def listen_stream(ott):
    """Listen for UDP packets simulating video frames."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", UDP_PORT))
    print(f"[CLIENT] Listening on UDP port {UDP_PORT} for stream data...")
    while True:
        data, addr = sock.recvfrom(4096)
        print(f"[STREAM] Packet from {addr}: {data[:40]!r}")


if __name__ == "__main__":
    main()
