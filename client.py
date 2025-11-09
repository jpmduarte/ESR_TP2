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
        return True
    except Exception as e:
        print(f"[ERROR] send_json failed: {e}")
        return False


def udp_probe(targets):
    results = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", UDP_PORT))
    sock.settimeout(1.0)
    
    for target in targets:
        addr = (target["ip"], target["port"])
        start = time.time()
        try:
            sock.sendto(b"PING", addr)
            data, _ = sock.recvfrom(1024)
            if data == b"PONG":
                rtt = (time.time() - start) * 1000
                results[target["id"]] = round(rtt, 2)
        except socket.timeout:
            results[target["id"]] = 9999
    
    sock.close()
    return results


class OTTClientGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("OTT Video Client")
        self.root.geometry("500x450")
        self.root.resizable(False, False)
        
        self.server_sock = None
        self.assigned_ott = None
        self.available_videos = []  
        self.stop_threads = threading.Event()
        self.active_streams = []  
        
        self._build_gui()
    
    def _build_gui(self):
        header = tk.Frame(self.root, bg="#2c3e50", height=60)
        header.pack(fill="x")
        header.pack_propagate(False)
        
        tk.Label(
            header,
            text="🎬 OTT Video Streaming Client",
            font=("Arial", 16, "bold"),
            bg="#2c3e50",
            fg="white"
        ).pack(pady=15)
        
        status_frame = tk.Frame(self.root, bg="#ecf0f1", height=40)
        status_frame.pack(fill="x", pady=(10, 0))
        status_frame.pack_propagate(False)
        
        self.status_label = tk.Label(
            status_frame,
            text="● Disconnected",
            font=("Arial", 10),
            bg="#ecf0f1",
            fg="#e74c3c"
        )
        self.status_label.pack(pady=10)
        
        self.connect_btn = tk.Button(
            self.root,
            text="Connect to Server",
            command=self.connect_server,
            bg="#3498db",
            fg="white",
            font=("Arial", 10, "bold"),
            relief=tk.FLAT,
            cursor="hand2"
        )
        self.connect_btn.pack(pady=10)
        
        ttk.Separator(self.root, orient="horizontal").pack(fill="x", pady=10)
        
        tk.Label(
            self.root,
            text="Available Streams:",
            font=("Arial", 11, "bold")
        ).pack(anchor="w", padx=20)
        
        list_frame = tk.Frame(self.root)
        list_frame.pack(fill="both", expand=True, padx=20, pady=5)
        
        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side="right", fill="y")
        
        self.video_list = tk.Listbox(
            list_frame,
            height=6,
            font=("Courier", 9),
            yscrollcommand=scrollbar.set
        )
        self.video_list.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=self.video_list.yview)
        
        self.play_btn = tk.Button(
            self.root,
            text="▶ Play Stream",
            command=self.request_stream,
            state=tk.DISABLED,
            bg="#27ae60",
            fg="white",
            font=("Arial", 10, "bold"),
            relief=tk.FLAT,
            cursor="hand2"
        )
        self.play_btn.pack(pady=10)
        
        tk.Label(
            self.root,
            text="Activity Log:",
            font=("Arial", 10, "bold")
        ).pack(anchor="w", padx=20)
        
        log_frame = tk.Frame(self.root)
        log_frame.pack(fill="both", expand=True, padx=20, pady=(0, 10))
        
        log_scroll = tk.Scrollbar(log_frame)
        log_scroll.pack(side="right", fill="y")
        
        self.log = tk.Text(
            log_frame,
            height=8,
            state=tk.DISABLED,
            font=("Courier", 8),
            yscrollcommand=log_scroll.set
        )
        self.log.pack(side="left", fill="both", expand=True)
        log_scroll.config(command=self.log.yview)
    
    def log_msg(self, text):
        timestamp = time.strftime("%H:%M:%S")
        self.log.config(state=tk.NORMAL)
        self.log.insert(tk.END, f"[{timestamp}] {text}\n")
        self.log.see(tk.END)
        self.log.config(state=tk.DISABLED)
    
    
    def connect_server(self):
        self.connect_btn.config(state=tk.DISABLED)
        self.status_label.config(text="● Connecting...", fg="#f39c12")
        self.root.update_idletasks()
        
        try:
            self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_sock.settimeout(5)
            self.server_sock.connect((SERVER_IP, SERVER_PORT))
            self.server_sock.settimeout(None)
            
            self.status_label.config(text=f"● Connected to {SERVER_IP}:{SERVER_PORT}", fg="#27ae60")
            self.log_msg(f"Connected to server {SERVER_IP}:{SERVER_PORT}")
            
            threading.Thread(target=self.listen_server, daemon=True).start()
            
            local_ip = self.server_sock.getsockname()[0]
            reg_msg = {
                "type": "REGISTER_CLIENT",
                "ip": local_ip,
                "port": UDP_PORT
            }
            send_json(self.server_sock, reg_msg)
            self.log_msg("Registration sent to server")
        
        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            messagebox.showerror("Connection Error", f"Failed to connect:\n{e}")
            self.status_label.config(text="● Disconnected", fg="#e74c3c")
            self.connect_btn.config(state=tk.NORMAL)
            if self.server_sock:
                try:
                    self.server_sock.close()
                except:
                    pass
    
    def listen_server(self):
        buffer = ""
        try:
            while not self.stop_threads.is_set():
                data = self.server_sock.recv(4096)
                if not data:
                    raise ConnectionResetError("Server closed connection")
                
                buffer += data.decode()
                
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if not line.strip():
                        continue
                    
                    try:
                        msg = json.loads(line)
                        self.process_message(msg)
                    except json.JSONDecodeError:
                        continue
        
        except Exception as e:
            self.log_msg(f"Connection lost: {e}")
            self.status_label.config(text="● Disconnected", fg="#e74c3c")
            self.connect_btn.config(state=tk.NORMAL)
            try:
                self.server_sock.close()
            except:
                pass
    
    def process_message(self, msg):
        msg_type = msg.get("type")
        
        if msg_type == "CLIENT_PROBE_TARGETS":
            targets = msg.get("targets", [])
            self.log_msg(f"Received {len(targets)} OTT nodes to probe")
            
            results = udp_probe(targets)
            
            send_json(self.server_sock, {
                "type": "CLIENT_PROBE_RESULTS",
                "results": results
            })
            self.log_msg("RTT measurements sent to server")
        
        elif msg_type == "ASSIGN_OTT":
            self.assigned_ott = msg["ott"]
            metric = msg.get("metric", {})
            rtt = metric.get("rtt_ms", "N/A")
            
            self.log_msg(
                f"Assigned to OTT {self.assigned_ott['id']} "
                f"({self.assigned_ott['ip']}) - RTT: {rtt} ms"
            )
            
            send_json(self.server_sock, {"type": "LIST_STREAMS"})
            self.log_msg("Requesting stream list...")
        
        elif msg_type == "STREAM_LIST":
            self.available_videos = msg.get("videos", [])
            self.video_list.delete(0, tk.END)
            
            for video in self.available_videos:
                display = f"{video['id']}. {video['name']} ({video['group']}:{video['port']})"
                self.video_list.insert(tk.END, display)
            
            self.play_btn.config(state=tk.NORMAL)
            self.log_msg(f"Received {len(self.available_videos)} available streams")
        
        elif msg_type == "STREAM_READY":
            video = msg["video"]
            group = msg["group"]
            port = msg["port"]
            ott_id = msg.get("ott_id")
            
            self.log_msg(f"Stream ready: {video} via {ott_id}")
            self.log_msg(f"Joining multicast {group}:{port}")
            
            threading.Thread(
                target=self.listen_multicast,
                args=(video, group, port),
                daemon=True
            ).start()
        
        elif msg_type == "ERROR":
            reason = msg.get("reason", "Unknown error")
            self.log_msg(f"ERROR: {reason}")
            messagebox.showerror("Server Error", reason)
    
    def request_stream(self):
        if not self.assigned_ott:
            messagebox.showwarning("Warning", "No OTT assigned yet")
            return
        
        selection = self.video_list.curselection()
        if not selection:
            messagebox.showinfo("Info", "Please select a video")
            return
        
        video = self.available_videos[selection[0]]
        
        send_json(self.server_sock, {
            "type": "REQUEST_STREAM",
            "video_id": video["id"],
            "ott_id": self.assigned_ott["id"]
        })
        
        self.log_msg(f"Requested stream: {video['name']}")
    
    def listen_multicast(self, video_name, group, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("", port))
            
            mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            self.active_streams.append(sock)
            self.log_msg(f"✓ Receiving stream: {video_name}")
            
            packet_count = 0
            last_log = time.time()
            
            while True:
                data, _ = sock.recvfrom(65507)
                packet_count += 1
                
                now = time.time()
                if now - last_log >= 2.0:
                    self.log_msg(f"[{video_name}] Received {packet_count} packets ({len(data)} bytes/packet)")
                    last_log = now
        
        except Exception as e:
            self.log_msg(f"Multicast error: {e}")
        finally:
            try:
                sock.close()
            except:
                pass
    
    def on_closing(self):
        self.stop_threads.set()
        
        for sock in self.active_streams:
            try:
                sock.close()
            except:
                pass
        
        if self.server_sock:
            try:
                self.server_sock.close()
            except:
                pass
        
        self.root.destroy()



if __name__ == "__main__":
    root = tk.Tk()
    app = OTTClientGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()