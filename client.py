import socket
import json
import threading
import random
import struct
import tkinter as tk
from tkinter import messagebox, ttk
import time

SERVER_IP = "10.0.0.10"
SERVER_PORT = 5000
UDP_PORT = random.randint(7000, 8000)


def send_json(sock, msg):
    try:
        sock.send((json.dumps(msg) + "\n").encode())
    except Exception as e:
        print(f"[ERRO] Falha ao enviar: {e}")


def udp_probe(targets):
    """Mede RTT UDP para os OTTs (PING/PONG)."""
    results = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", UDP_PORT))
    sock.settimeout(1.0)

    for t in targets:
        addr = (t["ip"], t["port"])
        start = time.time()
        try:
            sock.sendto(b"PING", addr)
            data, _ = sock.recvfrom(1024)
            if data == b"PONG":
                rtt = (time.time() - start) * 1000
                results[t["id"]] = round(rtt, 2)
        except socket.timeout:
            results[t["id"]] = 9999
    sock.close()
    return results


class OTTClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("OTT Client")
        self.root.geometry("420x350")
        self.root.resizable(False, False)

        self.server_sock = None
        self.assigned_ott = None
        self.stop_threads = threading.Event()

        self._build_gui()

    def _build_gui(self):
        tk.Label(self.root, text="OTT Video Client", font=("Arial", 14, "bold")).pack(pady=10)
        self.status_label = tk.Label(self.root, text="Desconectado do servidor.", fg="red")
        self.status_label.pack()

        self.connect_btn = tk.Button(self.root, text="Ligar ao Servidor", command=self.connect_server)
        self.connect_btn.pack(pady=5)

        ttk.Separator(self.root, orient="horizontal").pack(fill="x", pady=10)
        tk.Label(self.root, text="Streams disponíveis:").pack()

        self.video_list = tk.Listbox(self.root, height=6)
        self.video_list.pack(fill="x", padx=30, pady=5)

        self.play_btn = tk.Button(self.root, text="▶️ Reproduzir", command=self.request_stream, state=tk.DISABLED)
        self.play_btn.pack(pady=10)

        self.log = tk.Text(self.root, height=8, state=tk.DISABLED)
        self.log.pack(fill="both", padx=10, pady=5)

    def log_msg(self, text):
        self.log.config(state=tk.NORMAL)
        self.log.insert(tk.END, f"{text}\n")
        self.log.see(tk.END)
        self.log.config(state=tk.DISABLED)

    # ---------------------- Rede ----------------------
    def connect_server(self):
        self.connect_btn.config(state=tk.DISABLED)
        self.status_label.config(text="A ligar ao servidor...", fg="orange")
        self.root.update_idletasks()

        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.settimeout(5)
            self.server_sock.connect((SERVER_IP, SERVER_PORT))
            self.server_sock.settimeout(None)

            self.status_label.config(text=f"Ligado a {SERVER_IP}:{SERVER_PORT}", fg="green")
            self.log_msg("[CLIENT] Ligação estabelecida.")
            threading.Thread(target=self.listen_server, daemon=True).start()

            local_ip = self.server_sock.getsockname()[0]
            reg = {"type": "REGISTER_CLIENT", "ip": local_ip, "port": UDP_PORT}
            send_json(self.server_sock, reg)
            self.log_msg("[CLIENT] Pedido de registo enviado ao servidor.")

        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            messagebox.showwarning("Falha de Conexão", f"Não foi possível ligar ao servidor:\n{e}")
            self.status_label.config(text="Desconectado do servidor.", fg="red")
            self.connect_btn.config(state=tk.NORMAL)
            if self.server_sock:
                try:
                    self.server_sock.close()
                except:
                    pass
            return

    def listen_server(self):
        buffer = ""
        try:
            while not self.stop_threads.is_set():
                data = self.server_sock.recv(4096)
                if not data:
                    raise ConnectionResetError("Servidor fechou a ligação.")
                buffer += data.decode()

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    msg = json.loads(line)
                    self.process_message(msg)
        except Exception as e:
            self.log_msg(f"[ERRO] Ligação encerrada: {e}")
            self.status_label.config(text="Desconectado do servidor.", fg="red")
            self.connect_btn.config(state=tk.NORMAL)
            try:
                self.server_sock.close()
            except:
                pass

    def process_message(self, msg):
        t = msg.get("type")

        if t == "CLIENT_PROBE_TARGETS":
            targets = msg.get("targets", [])
            self.log_msg(f"[CLIENT] {len(targets)} nós OTT para testar.")
            results = udp_probe(targets)
            send_json(self.server_sock, {"type": "CLIENT_PROBE_RESULTS", "results": results})
            self.log_msg("[CLIENT] Resultados RTT enviados.")

        elif t == "ASSIGN_OTT":
            self.assigned_ott = msg["ott"]
            self.log_msg(f"[CLIENT] OTT atribuído: {self.assigned_ott['id']} ({self.assigned_ott['ip']})")
            send_json(self.server_sock, {"type": "LIST_STREAMS"})
            self.log_msg("[CLIENT] A pedir lista de vídeos...")

        elif t == "STREAM_LIST":
            self.video_list.delete(0, tk.END)
            for v in msg.get("videos", []):
                self.video_list.insert(tk.END, v)
            self.play_btn.config(state=tk.NORMAL)
            self.log_msg(f"[CLIENT] {len(msg.get('videos', []))} vídeos disponíveis.")

        elif t == "STREAM_READY":
            group = msg["group"]
            port = msg["port"]
            video = msg["video"]
            self.log_msg(f"[STREAM] A reproduzir {video} ({group}:{port}) ...")
            threading.Thread(target=self.listen_multicast, args=(group, port), daemon=True).start()

        elif t == "ERROR":
            self.log_msg(f"[ERRO SERVIDOR] {msg.get('reason')}")

    def request_stream(self):
        if not self.assigned_ott:
            messagebox.showwarning("Aviso", "Nenhum OTT atribuído ainda.")
            return

        selection = self.video_list.curselection()
        if not selection:
            messagebox.showinfo("Info", "Selecione um vídeo da lista.")
            return

        video_name = self.video_list.get(selection[0])
        send_json(self.server_sock, {
            "type": "REQUEST_STREAM",
            "video": video_name,
            "ott_id": self.assigned_ott["id"]
        })
        self.log_msg(f"[CLIENT] Pedido de stream: {video_name}")

    def listen_multicast(self, group, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", port))
        mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.log_msg(f"[CLIENT] Joined multicast {group}:{port}")

        while True:
            data, _ = sock.recvfrom(8192)
            self.log_msg(f"[FRAME] {len(data)} bytes recebidos")


# -------------------------------------------------------------------
# Execução
# -------------------------------------------------------------------
if __name__ == "__main__":
    root = tk.Tk()
    app = OTTClientGUI(root)
    root.mainloop()
