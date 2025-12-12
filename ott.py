#!/usr/bin/env python3
import socket
import threading
import json
from datetime import datetime
import sys
import time
import heapq
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import cv2
import numpy as np
import struct
import random

BOOTSTRAP_PORT = 5000
BASE_TCP_PORT  = 6000
BASE_UDP_PORT  = 8000
PING_INTERVAL  = 2    # seconds between PINGs (faster for video)

# Routing / LSA parameters
LSA_INTERVAL   = 10
MIN_LSA_GAP    = 3
MAX_LSA_AGE    = 60
RTT_ALPHA      = 0.3
RTT_CHANGE_EPS = 0.005

# RTP constants
RTP_VERSION = 2
RTP_PAYLOAD_TYPE = 96  # Dynamic payload type for H.264

def log(tag, msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}][{tag}] {msg}", flush=True)

def send_json(sock, obj):
    try:
        data = json.dumps(obj) + "\n"
        sock.sendall(data.encode())
        return True
    except Exception as e:
        log("ERR", f"send_json error: {e}")
        return False

def recv_lines(sock, buf):
    try:
        sock.settimeout(0.1)
        data = sock.recv(4096)
        if not data:
            return None, buf
        buf += data.decode()
        lines = []
        while "\n" in buf:
            ln, buf = buf.split("\n", 1)
            if ln.strip():
                lines.append(ln)
        return lines, buf
    except socket.timeout:
        return [], buf
    except Exception:
        return None, buf

def close_socket(s):
    if not s:
        return
    try:
        s.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        s.close()
    except Exception:
        pass


class RTPPacket:
    """Simple RTP packet encoder/decoder for video streaming."""
    
    def __init__(self, payload_type=RTP_PAYLOAD_TYPE):
        self.payload_type = payload_type
        self.sequence = random.randint(0, 65535)
        self.timestamp = 0
        self.ssrc = random.randint(0, 0xFFFFFFFF)
    
    def create_packet(self, payload, marker=False):
        """Create an RTP packet with the given payload."""
        # RTP Header: V(2) P(1) X(1) CC(4) M(1) PT(7) Sequence(16) Timestamp(32) SSRC(32)
        header = bytearray(12)
        
        # Byte 0: V(2) P(1) X(1) CC(4)
        header[0] = (RTP_VERSION << 6)
        
        # Byte 1: M(1) PT(7)
        header[1] = (1 if marker else 0) << 7 | (self.payload_type & 0x7F)
        
        # Bytes 2-3: Sequence number
        struct.pack_into('!H', header, 2, self.sequence)
        self.sequence = (self.sequence + 1) % 65535
        
        # Bytes 4-7: Timestamp
        struct.pack_into('!I', header, 4, self.timestamp)
        
        # Bytes 8-11: SSRC
        struct.pack_into('!I', header, 8, self.ssrc)
        
        return bytes(header) + payload
    
    @staticmethod
    def parse_packet(data):
        """Parse an RTP packet and return (sequence, timestamp, payload)."""
        if len(data) < 12:
            return None
        
        sequence = struct.unpack('!H', data[2:4])[0]
        timestamp = struct.unpack('!I', data[4:8])[0]
        payload = data[12:]
        
        return sequence, timestamp, payload


class ClientGUI:
    def __init__(self, node):
        self.node = node
        self.root = tk.Tk()
        self.root.title(f"Node {node.node_id} - Overlay Network Video Streaming")
        self.root.geometry("1000x700")

        self.streams = []
        self.currently_playing = None

        self.create_widgets()
        
        self.root.after(1000, self.refresh_stream_list)
        self.root.after(500, self.update_stats)
        
        self.video_thread = threading.Thread(target=self.video_display_loop, daemon=True)
        self.video_thread.start()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def create_widgets(self):
        # Left panel
        left_frame = ttk.Frame(self.root, padding="10")
        left_frame.grid(row=0, column=0, sticky=(tk.N, tk.S, tk.W, tk.E), rowspan=2)
        
        ttk.Label(left_frame, text="Available Streams", font=('Arial', 12, 'bold')).pack(pady=5)
        
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.stream_listbox = tk.Listbox(list_frame, width=30, height=10, yscrollcommand=scrollbar.set)
        self.stream_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.stream_listbox.yview)
        
        btn_frame = ttk.Frame(left_frame)
        btn_frame.pack(pady=10)
        
        self.btn_join = ttk.Button(btn_frame, text="▶ Start Viewing",
                                   command=self.start_viewing, width=20)
        self.btn_join.pack(pady=5)
        
        self.btn_leave = ttk.Button(btn_frame, text="⏹ Stop Viewing",
                                    command=self.stop_viewing, width=20, state=tk.DISABLED)
        self.btn_leave.pack(pady=5)
        
        ttk.Separator(left_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=10)
        ttk.Label(left_frame, text="Network Status", font=('Arial', 10, 'bold')).pack()
        
        self.status_text = scrolledtext.ScrolledText(left_frame, width=30, height=10,
                                                     wrap=tk.WORD, state=tk.DISABLED)
        self.status_text.pack(pady=5, fill=tk.BOTH, expand=True)
        
        # Right panel
        right_frame = ttk.Frame(self.root, padding="10")
        right_frame.grid(row=0, column=1, sticky=(tk.N, tk.S, tk.W, tk.E))
        
        self.canvas_frame = ttk.Frame(right_frame, relief=tk.SUNKEN, borderwidth=2)
        self.canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.canvas = tk.Label(self.canvas_frame, text="No stream playing",
                               bg='black', fg='white', font=('Arial', 16))
        self.canvas.pack(fill=tk.BOTH, expand=True)
        
        info_frame = ttk.Frame(right_frame)
        info_frame.pack(fill=tk.X, pady=10)
        
        self.info_label = ttk.Label(info_frame, text="Ready to stream", font=('Arial', 10))
        self.info_label.pack()
        
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

    def refresh_stream_list(self):
        with self.node.lock:
            streams = list(self.node.mtree.stream_sources.keys())

        if streams != self.streams:
            self.streams = streams
            self.stream_listbox.delete(0, tk.END)
            for s in streams:
                src = self.node.mtree.stream_sources.get(s, "Unknown")
                self.stream_listbox.insert(tk.END, f"{s} (from {src})")

        self.root.after(1000, self.refresh_stream_list)

    def update_stats(self):
        with self.node.lock:
            neighbors = len(self.node.neighbors_out)
            routes = len(self.node.routes)
            
        stats = [
            f"Node ID: {self.node.node_id}",
            f"Neighbors: {neighbors}",
            f"Routes: {routes}",
        ]
        
        if self.currently_playing:
            stats.append("")
            stats.append(f"Active Stream: {self.currently_playing}")
            with self.node.lock:
                tree = self.node.mtree.stream_trees.get(self.currently_playing)
                if tree:
                    stats.append(f"Parent: {tree['parent'] or 'N/A'}")
                    stats.append(f"Children: {len(tree['children'])}")
            stats.append(f"Frames: {self.node.frames_received}")
            stats.append(f"FPS: {self.node.current_fps:.1f}")
        
        self.status_text.config(state=tk.NORMAL)
        self.status_text.delete(1.0, tk.END)
        self.status_text.insert(1.0, "\n".join(stats))
        self.status_text.config(state=tk.DISABLED)
        
        self.root.after(500, self.update_stats)

    def start_viewing(self):
        selection = self.stream_listbox.curselection()
        if not selection:
            messagebox.showwarning("Select Stream", "Please select a stream to view.")
            return
        
        sid = self.streams[selection[0]]
        self.currently_playing = sid
        self.node.client_stream_id = sid
        self.node.is_client = True
        
        self.node.frames_received = 0
        self.node.frame_times = []
        
        self.node.mtree.join_stream(sid)
        self.btn_leave.config(state=tk.NORMAL)
        self.btn_join.config(state=tk.DISABLED)
        self.info_label.config(text=f"Streaming: {sid}")
        log(self.node.node_id, f"Started viewing stream {sid}")

    def stop_viewing(self):
        if not self.currently_playing:
            return
        
        sid = self.currently_playing
        
        with self.node.lock:
            tree = self.node.mtree.stream_trees.get(sid)
            if tree:
                tree["local_sink"] = False
        
        self.node.mtree.leave_stream(sid)
        
        self.currently_playing = None
        self.btn_leave.config(state=tk.DISABLED)
        self.btn_join.config(state=tk.NORMAL)
        self.info_label.config(text="Ready to stream")
        
        self.canvas.config(image='', text='No stream playing', bg='black', fg='white')
        
        with self.node.last_frame_lock:
            self.node.last_frame = None
        
        log(self.node.node_id, f"Stopped viewing stream {sid}")

    def video_display_loop(self):
        """Display video directly in GUI canvas using Tkinter PhotoImage."""
        while self.node.running:
            try:
                with self.node.last_frame_lock:
                    frame = self.node.last_frame.copy() if self.node.last_frame is not None else None
                
                if frame is not None:
                    h, w = frame.shape[:2]
                    max_h, max_w = 480, 640
                    if h > max_h or w > max_w:
                        scale = min(max_w / w, max_h / h)
                        frame = cv2.resize(frame, None, fx=scale, fy=scale)
                    
                    frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    h, w = frame_rgb.shape[:2]
                    ppm_header = f'P6 {w} {h} 255 '.encode()
                    ppm_data = ppm_header + frame_rgb.tobytes()
                    
                    photo = tk.PhotoImage(width=w, height=h, data=ppm_data, format='PPM')
                    
                    def update_canvas():
                        self.canvas.photo = photo
                        self.canvas.configure(image=photo, text='', bg='black')
                    
                    self.root.after(0, update_canvas)
                else:
                    def show_no_stream():
                        self.canvas.configure(image='', text='Waiting for stream...',
                                              bg='black', fg='white')
                    self.root.after(0, show_no_stream)
                
                time.sleep(0.03)
            except Exception as e:
                log(self.node.node_id, f"Video display error: {e}")
                time.sleep(0.1)

    def on_close(self):
        log(self.node.node_id, "GUI closing...")
        if self.currently_playing:
            self.stop_viewing()
        self.root.quit()
        self.root.destroy()

    def run(self):
        self.root.mainloop()


class BootstrapServer:
    def __init__(self, topo_file):
        self.topology = self.load_topology(topo_file)
        self.registered = {}
        self.lock = threading.Lock()

    def load_topology(self, fname):
        with open(fname) as f:
            data = json.load(f)
        topo = data.get("nodes", {})
        log("BOOT", f"Topology loaded with {len(topo)} nodes.")
        return topo

    def compute_active_neighbors_for(self, nid):
        topo_entry = self.topology.get(nid)
        if topo_entry is None:
            log("BOOT", f"WARNING: node {nid} not found in topology.json")
            return []

        neighbors = []
        with self.lock:
            for nbr_id in topo_entry.get("neighbors", []):
                info = self.registered.get(nbr_id)
                if info:
                    neighbors.append({
                        "id": nbr_id,
                        "ip": info["ip"],
                        "tcp_port": info["tcp_port"],
                        "udp_port": info["udp_port"],
                    })
        return neighbors

    def _notify_neighbors(self, nid, payload_builder):
        topo_entry = self.topology.get(nid)
        if topo_entry is None:
            return

        with self.lock:
            for nbr_id in topo_entry.get("neighbors", []):
                nbr_info = self.registered.get(nbr_id)
                if not nbr_info:
                    continue
                send_json(nbr_info["sock"], payload_builder(nbr_id))

    def notify_neighbors_new_node(self, nid):
        def builder(_):
            info = self.registered.get(nid)
            return {
                "type": "NEW_NODE",
                "node": {
                    "id": nid,
                    "ip": info["ip"],
                    "tcp_port": info["tcp_port"],
                    "udp_port": info["udp_port"],
                }
            }
        self._notify_neighbors(nid, builder)

    def notify_neighbors_dead_node(self, nid):
        def builder(_):
            return {"type": "DEAD_NODE", "node_id": nid}
        self._notify_neighbors(nid, builder)

    def run(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", BOOTSTRAP_PORT))
        srv.listen(50)
        log("BOOT", f"Bootstrap listening on :{BOOTSTRAP_PORT}")

        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr), daemon=True).start()

    def handle_connection(self, sock, addr):
        buf = ""
        node_id = None

        try:
            while True:
                lines, buf = recv_lines(sock, buf)
                if lines is None:
                    break

                for ln in lines:
                    msg = json.loads(ln)
                    if msg.get("type") == "REGISTER" and node_id is None:
                        node_id = msg["node_id"]
                        tcp_port = msg["tcp_port"]
                        udp_port = msg.get("udp_port", 0)
                        ip = msg.get("ip") or addr[0]
                        self.register_node(node_id, ip, tcp_port, udp_port, sock)
        except Exception as e:
            log("BOOT", f"Error in connection handler: {e}")
        finally:
            if node_id is not None:
                self.unregister_node(node_id)
            close_socket(sock)

    def register_node(self, nid, ip, tcp_port, udp_port, sock):
        with self.lock:
            self.registered[nid] = {
                "ip": ip,
                "tcp_port": tcp_port,
                "udp_port": udp_port,
                "sock": sock,
            }

        log("BOOT", f"Node registered: {nid} ({ip}:{tcp_port}/{udp_port})")

        neighbors = self.compute_active_neighbors_for(nid)
        send_json(sock, {"type": "NEIGHBORS", "neighbors": neighbors})
        self.notify_neighbors_new_node(nid)

    def unregister_node(self, nid):
        with self.lock:
            info = self.registered.pop(nid, None)
        if info:
            log("BOOT", f"Node disconnected: {nid}")
            self.notify_neighbors_dead_node(nid)


class MulticastTree:
    def __init__(self, node):
        self.node = node
        self.stream_sources = {}
        self.stream_trees = {}
        self.stream_seen = set()
        self.pending_rejoins = set()

    def _log(self, msg):
        log(self.node.node_id, msg)

    def _ensure_tree(self, sid):
        tree = self.stream_trees.get(sid)
        if tree is None:
            src = self.stream_sources.get(sid)
            tree = {
                "source": src,
                "parent": None,
                "children": set(),
                "local_sink": False,
            }
            self.stream_trees[sid] = tree
        return tree

    def has_subscribers(self, sid):
        with self.node.lock:
            tree = self.stream_trees.get(sid)
            return bool(tree and (tree["local_sink"] or tree["children"]))

    def announce_stream(self, sid):
        with self.node.lock:
            self.stream_sources[sid] = self.node.node_id
            self.stream_seen.add(sid)
            self.stream_trees[sid] = {
                "source": self.node.node_id,
                "parent": None,
                "children": set(),
                "local_sink": False,
            }

        msg = {"type": "STREAM_ANNOUNCE", "stream_id": sid, "source": self.node.node_id}
        self._flood_announce(msg, exclude=None)
        self._log(f"Announced stream {sid}")

    def _flood_announce(self, msg, exclude):
        with self.node.lock:
            peers = list(self.node.neighbors_out.items())
        for nid, sock in peers:
            if nid == exclude:
                continue
            send_json(sock, msg)

    def on_new_peer(self, nid, sock):
        with self.node.lock:
            streams = dict(self.stream_sources)
        for sid, src in streams.items():
            send_json(sock, {"type": "STREAM_ANNOUNCE", "stream_id": sid, "source": src})

    def handle_stream_announce(self, msg, from_nid):
        sid = msg["stream_id"]
        src = msg["source"]

        with self.node.lock:
            if sid in self.stream_seen:
                return
            self.stream_seen.add(sid)
            self.stream_sources[sid] = src

        self._log(f"Learned stream {sid} from {src} via {from_nid}")
        self._flood_announce(msg, exclude=from_nid)

    def join_stream(self, sid):
        """Join a stream as a viewer (local sink)."""
        with self.node.lock:
            src = self.stream_sources.get(sid)
            tree = self._ensure_tree(sid)

            was_sink = tree["local_sink"]
            tree["local_sink"] = True

            if src and tree["parent"] is None and src != self.node.node_id:
                nh = self.node.get_next_hop(src)
                if nh is not None:
                    tree["parent"] = nh
                    self._log(f"JOIN: set parent={nh} for local stream {sid}")

            self._log(
                f"JOIN: Joining stream {sid} - was_sink={was_sink}, "
                f"parent={tree['parent']}, children={list(tree['children'])}"
            )

        if not src:
            self._log(f"JOIN: No source known for stream {sid}")
            return

        if src == self.node.node_id:
            self._log(f"JOIN: We are the source for {sid}, not sending JOIN")
            return

        msg = {"type": "STREAM_JOIN", "stream_id": sid, "subscriber": self.node.node_id}
        self.node.send_to_node(src, msg)
        self._log(f"JOIN: Sent STREAM_JOIN for {sid} towards source {src}")

    def leave_stream(self, sid):
        """Explicitly leave a stream and notify parent."""
        parent = None
        has_children = False
        
        with self.node.lock:
            tree = self.stream_trees.get(sid)
            if not tree:
                self._log(f"LEAVE: No tree for stream {sid}")
                return
            
            was_sink = tree["local_sink"]
            tree["local_sink"] = False
            parent = tree["parent"]
            has_children = bool(tree["children"])
            
            self._log(
                f"LEAVE: Stream {sid} - was_sink={was_sink}, "
                f"parent={parent}, children={list(tree['children'])}"
            )

        if not has_children and parent:
            msg = {"type": "STREAM_LEAVE", "stream_id": sid, "subscriber": self.node.node_id}
            
            with self.node.lock:
                sock = self.node.neighbors_out.get(parent)
            
            if sock:
                send_json(sock, msg)
                self._log(f"LEAVE: Sent STREAM_LEAVE for {sid} to parent {parent}")
            else:
                self._log(f"LEAVE: No socket to parent {parent}")
            
            with self.node.lock:
                tree = self.stream_trees.get(sid)
                if tree:
                    tree["parent"] = None
                    self._log(f"LEAVE: Cleared parent for stream {sid}")
        else:
            self._log("LEAVE: Staying in tree as forwarder "
                      f"(has {len(self.stream_trees.get(sid, {}).get('children', []))} children)")

    def handle_stream_join(self, msg, from_nid):
        sid = msg["stream_id"]
        
        with self.node.lock:
            src = self.stream_sources.get(sid)
        if not src:
            self._log(f"STREAM_JOIN: Unknown stream {sid}")
            return

        tree = self._ensure_tree(sid)
        
        with self.node.lock:
            tree["children"].add(from_nid)
            self._log(f"STREAM_JOIN sid={sid}: added child={from_nid}, children={tree['children']}")

        if self.node.node_id == src:
            self._log("STREAM_JOIN: We are source, not forwarding")
            return

        nh = self.node.get_next_hop(src)
        if nh is None:
            self._log(f"STREAM_JOIN: No route to source {src}")
            return

        should_forward = False
        with self.node.lock:
            if tree["parent"] is None:
                tree["parent"] = nh
                should_forward = True
                self._log(f"STREAM_JOIN: Set parent={nh} for stream {sid}")
            else:
                self._log(f"STREAM_JOIN: Already have parent={tree['parent']}, not forwarding")

        if should_forward:
            self._log(f"STREAM_JOIN: Forwarding to source {src} via next-hop {nh}")
            self.node.send_to_node(src, msg)

    def handle_stream_leave(self, msg, from_nid):
        """Handle STREAM_LEAVE from a child."""
        sid = msg["stream_id"]
        
        parent = None
        should_propagate = False
        local_sink = False
        remaining_children = 0
        
        with self.node.lock:
            tree = self.stream_trees.get(sid)
            if not tree:
                self._log(f"LEAVE: No tree for stream {sid}")
                return
            
            if from_nid in tree["children"]:
                tree["children"].discard(from_nid)
                self._log(f"LEAVE: Removed child {from_nid} from stream {sid}")
            else:
                self._log(f"LEAVE: Child {from_nid} not in children list for stream {sid}")
            
            local_sink = tree["local_sink"]
            remaining_children = len(tree["children"])
            parent = tree["parent"]
            
            self._log(
                f"LEAVE: After removing {from_nid} - local_sink={local_sink}, "
                f"children={list(tree['children'])}, parent={parent}"
            )
            
            if not local_sink and remaining_children == 0:
                should_propagate = True
                tree["parent"] = None
                self._log(f"LEAVE: No more interest in {sid}, will propagate to parent {parent}")
        
        if should_propagate and parent:
            with self.node.lock:
                sock = self.node.neighbors_out.get(parent)
            
            if sock:
                leave_msg = {"type": "STREAM_LEAVE", "stream_id": sid,
                             "subscriber": self.node.node_id}
                send_json(sock, leave_msg)
                self._log(f"LEAVE: Propagated STREAM_LEAVE for {sid} to parent {parent}")
            else:
                self._log(f"LEAVE: No socket to parent {parent} for propagation")
        elif not should_propagate:
            self._log(f"LEAVE: Not propagating - local_sink={local_sink}, "
                      f"children={remaining_children}")


    def _rejoin_stream_after_parent_loss(self, sid):
        """
        Called when we lost our parent for stream `sid`.

        Goal:
          - If we (or our children) still care about this stream,
            find the new next-hop to the source using updated routes
            and send a fresh STREAM_JOIN towards the source.
          - If there's no route yet, mark as pending and retry when
            the routing table changes (on_routes_changed).
        """
        with self.node.lock:
            src   = self.stream_sources.get(sid)
            tree  = self.stream_trees.get(sid)
            routes = dict(self.node.routes)

        if not tree or not src or src == self.node.node_id:
            return

        if not tree["local_sink"] and not tree["children"]:
            with self.node.lock:
                self.pending_rejoins.discard(sid)
            self._log(f"REJOIN {sid}: no local sink and no children, not rejoining")
            return

        nh = None
        if src in routes:
            nh = routes[src][0]

        if nh is None:
            with self.node.lock:
                self.pending_rejoins.add(sid)
            self._log(
                f"REJOIN {sid}: no route to source {src} yet, "
                "will retry when routes change"
            )
            return

        with self.node.lock:
            tree["parent"] = nh
            self.pending_rejoins.discard(sid)

        join_msg = {
            "type": "STREAM_JOIN",
            "stream_id": sid,
            "subscriber": self.node.node_id,
        }
        self.node.send_to_node(src, join_msg)
        self._log(
            f"REJOIN {sid}: new parent={nh}, sent STREAM_JOIN towards source {src}"
        )

    def on_routes_changed(self):
        """
        Called after the overlay recomputes unicast routes.

        For each stream where we still have interest (local sink or children):
        - Ensure our multicast parent == current unicast next-hop to the source.
        - If the parent should change, send LEAVE to the old parent and JOIN
            towards the source via the new parent.
        - If we lost the route entirely, fall back to _rejoin_stream_after_parent_loss.
        """
        with self.node.lock:
            streams = list(self.stream_trees.items())
            routes = dict(self.node.routes)

        for sid, tree in streams:
            src = self.stream_sources.get(sid)
            if not src or src == self.node.node_id:
                continue

            interested = tree["local_sink"] or bool(tree["children"])
            if not interested:
                continue

            nh_entry = routes.get(src)
            new_parent = nh_entry[0] if nh_entry else None

            with self.node.lock:
                old_parent = tree["parent"]

            if new_parent is None:
                self._log(
                    f"ROUTES_CHANGED {sid}: lost route to src={src}, "
                    f"old_parent={old_parent}"
                )
                self._rejoin_stream_after_parent_loss(sid)
                continue

            if old_parent == new_parent:
                continue

            self._log(
                f"ROUTES_CHANGED {sid}: switching parent {old_parent} → {new_parent} "
                f"(src={src})"
            )

            if old_parent is not None:
                with self.node.lock:
                    sock = self.node.neighbors_out.get(old_parent)
                if sock:
                    leave = {
                        "type": "STREAM_LEAVE",
                        "stream_id": sid,
                        "subscriber": self.node.node_id,
                    }
                    send_json(sock, leave)
                    self._log(
                        f"ROUTES_CHANGED {sid}: sent LEAVE to old parent {old_parent}"
                    )

            with self.node.lock:
                tree = self.stream_trees.get(sid)
                if not tree:
                    continue
                tree["parent"] = new_parent

            join = {
                "type": "STREAM_JOIN",
                "stream_id": sid,
                "subscriber": self.node.node_id,
            }
            self.node.send_to_node(src, join)
            self._log(
                f"ROUTES_CHANGED {sid}: sent JOIN towards src={src} via {new_parent}"
            )

        with self.node.lock:
            pending = list(self.pending_rejoins)
        for sid in pending:
            self._rejoin_stream_after_parent_loss(sid)


    def on_neighbor_gone(self, nid):
        """
        Called whenever an overlay neighbor disappears (TCP closed or DEAD_NODE).
        - Prunes it from children.
        - If it was parent: triggers rejoin.
        - If losing this child leaves us with no sink and no children,
          propagate STREAM_LEAVE upstream.
        """
        lost_parent_streams = []
        to_propagate = []

        with self.node.lock:
            for sid, tree in self.stream_trees.items():
                changed_child = False

                if nid in tree["children"]:
                    tree["children"].discard(nid)
                    changed_child = True
                    self._log(f"Neighbor gone: pruned child {nid} from stream {sid}")

                if tree["parent"] == nid:
                    tree["parent"] = None
                    lost_parent_streams.append(sid)
                    self._log(f"Neighbor gone: lost parent {nid} for stream {sid}")

                if changed_child and not tree["local_sink"] and not tree["children"] and tree["parent"]:
                    parent = tree["parent"]
                    tree["parent"] = None
                    to_propagate.append((sid, parent))
                    self._log(
                        f"Neighbor gone: no more interest in {sid}, "
                        f"will send LEAVE to parent {parent}"
                    )

        for sid in lost_parent_streams:
            self._rejoin_stream_after_parent_loss(sid)

        for sid, parent in to_propagate:
            leave_msg = {"type": "STREAM_LEAVE", "stream_id": sid, "subscriber": self.node.node_id}
            with self.node.lock:
                sock = self.node.neighbors_out.get(parent)
            if sock:
                send_json(sock, leave_msg)
                self._log(f"Neighbor gone: propagated STREAM_LEAVE for {sid} to parent {parent}")
            else:
                self._log(f"Neighbor gone: no socket to parent {parent} to propagate LEAVE for {sid}")

    def handle_rtp_packet(self, data, from_addr):
        """Handle incoming RTP video packet."""
        if len(data) < 16:
            return
        
        sid = data[:4].decode('ascii', errors='ignore').strip('\x00')
        rtp_data = data[4:]
        
        parsed = RTPPacket.parse_packet(rtp_data)
        if not parsed:
            return
        
        seq, timestamp, payload = parsed  

        with self.node.lock:
            tree = self.stream_trees.get(sid) or self._ensure_tree(sid)
            local_sink = tree["local_sink"]
            children = set(tree["children"])
            neighbors_cfg = dict(self.node.neighbors_cfg)
            
        if local_sink:
            try:
                np_arr = np.frombuffer(payload, dtype=np.uint8)
                frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                if frame is not None:
                    with self.node.last_frame_lock:
                        self.node.last_frame = frame
                    
                    self.node.frames_received += 1
                    now = time.time()
                    self.node.frame_times.append(now)
                    if len(self.node.frame_times) > 30:
                        self.node.frame_times.pop(0)
                    if len(self.node.frame_times) > 1:
                        elapsed = self.node.frame_times[-1] - self.node.frame_times[0]
                        if elapsed > 0:
                            self.node.current_fps = (len(self.node.frame_times) - 1) / elapsed
                    
                    if self.node.frames_received % 30 == 0:
                        self._log(f"Received frame {self.node.frames_received} for stream {sid}")
            except Exception as e:
                self._log(f"Video decode error: {e}")
        
        for child in children:
            info = neighbors_cfg.get(child)
            if not info:
                continue
            try:
                self.node.udp_sock.sendto(data, (info["ip"], info["udp_port"]))
            except Exception as e:
                self._log(f"RTP forward error to {child}: {e}")

    def start_video_stream(self, sid, video_path):
        with self.node.lock:
            if sid in self.stream_trees:
                return
        
        self.announce_stream(sid)
        threading.Thread(target=self._video_stream_loop, args=(sid, video_path), daemon=True).start()

    def _video_stream_loop(self, sid, video_path):
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            self._log(f"ERROR: cannot open video file {video_path}")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)
        if fps <= 0 or fps > 60:
            fps = 25.0
        frame_interval = 1.0 / fps

        rtp = RTPPacket()
        frame_count = 0
        last_subscriber_check = 0
        
        self._log(f"Video stream loop started for {sid}, fps={fps}")
        
        while self.node.running:
            has_subs = self.has_subscribers(sid)
            
            if time.time() - last_subscriber_check > 5:
                with self.node.lock:
                    tree = self.stream_trees.get(sid)
                    if tree:
                        self._log(
                            f"Stream {sid}: has_subscribers={has_subs}, "
                            f"children={tree['children']}, local_sink={tree['local_sink']}"
                        )
                last_subscriber_check = time.time()
            
            if not has_subs:
                time.sleep(0.5)
                continue
            
            ret, frame = cap.read()
            if not ret:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                continue

            frame = cv2.resize(frame, (640, 480))
            ok, buf = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 75])
            if not ok:
                continue
            
            payload = buf.tobytes()
            
            rtp.timestamp = int(frame_count * (90000 / fps))  # 90kHz clock
            rtp_packet = rtp.create_packet(payload, marker=True)
            
            sid_bytes = sid.ljust(4, '\x00').encode('ascii')[:4]
            full_packet = sid_bytes + rtp_packet
            
            with self.node.lock:
                tree = self.stream_trees.get(sid)
                if not tree:
                    break
                children = set(tree["children"])
                neighbors_cfg = dict(self.node.neighbors_cfg)

            if not children:
                time.sleep(frame_interval)
                frame_count += 1
                continue

            sent_count = 0
            for child in children:
                info = neighbors_cfg.get(child)
                if not info:
                    continue
                try:
                    self.node.udp_sock.sendto(full_packet, (info["ip"], info["udp_port"]))
                    sent_count += 1
                except Exception as e:
                    self._log(f"RTP send error to {child}: {e}")

            if frame_count % 30 == 0:
                self._log(f"Sent frame {frame_count} to {sent_count} children: {children}")

            frame_count += 1
            time.sleep(frame_interval)

        cap.release()
        self._log(f"Video stream loop ended for {sid}")


class OverlayNode:
    def __init__(self, node_id, tcp_port, udp_port, bootstrap_ip,
                 is_source=False, client_stream_id=None, video_path=None):
        self.node_id = node_id
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.bootstrap_ip = bootstrap_ip
        self.is_source = is_source
        self.client_stream_id = client_stream_id
        self.video_path = video_path

        self.neighbors_cfg = {}
        self.neighbors_out = {}
        self.neighbors_in = {}

        self.bootstrap_sock = None
        self.running = True
        self.lock = threading.RLock()

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2097152)
        self.udp_sock.bind(("0.0.0.0", self.udp_port))

        self.rtt = {}
        self.lsdb = {}
        self.lsa_seq = 0
        self.last_lsa_ts = 0
        self.last_lsa_neighbors = {}
        self.routes = {}

        self.mtree = MulticastTree(self)

        self.source_stream_started = False
        self.client_joined = False

        self.last_frame = None
        self.last_frame_lock = threading.Lock()
        self.is_client = (self.client_stream_id is not None)
        
        self.frames_received = 0
        self.frame_times = []
        self.current_fps = 0.0

    def register_to_bootstrap(self):
        s = socket.socket()
        s.connect((self.bootstrap_ip, BOOTSTRAP_PORT))
        self.bootstrap_sock = s

        local_ip = s.getsockname()[0]
        send_json(s, {
            "type": "REGISTER",
            "node_id": self.node_id,
            "ip": local_ip,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
        })

        log(self.node_id, f"Registered to bootstrap @ {self.bootstrap_ip}:{BOOTSTRAP_PORT}")
        threading.Thread(target=self.bootstrap_recv_loop, args=(s,), daemon=True).start()

    def bootstrap_recv_loop(self, sock):
        buf = ""
        while self.running:
            lines, buf = recv_lines(sock, buf)
            if lines is None:
                log(self.node_id, "Lost connection to bootstrap")
                break

            for ln in lines:
                try:
                    msg = json.loads(ln)
                    t = msg.get("type")
                except Exception:
                    continue

                if t == "NEIGHBORS":
                    for nb in msg["neighbors"]:
                        self.add_neighbor(nb)
                elif t == "NEW_NODE":
                    self.add_neighbor(msg["node"])
                elif t == "DEAD_NODE":
                    self.remove_neighbor(msg["node_id"])

    def add_neighbor(self, nb):
        nid = nb["id"]
        if nid == self.node_id:
            return

        with self.lock:
            self.neighbors_cfg[nid] = {
                "ip": nb["ip"],
                "tcp_port": nb["tcp_port"],
                "udp_port": nb["udp_port"],
            }
        log(self.node_id, f"Learned neighbor {nid}")

    def remove_neighbor(self, nid):
        with self.lock:
            self.neighbors_cfg.pop(nid, None)
            s_out = self.neighbors_out.pop(nid, None)
            s_in = self.neighbors_in.pop(nid, None)
            self.rtt.pop(nid, None)

        close_socket(s_out)
        close_socket(s_in)

        log(self.node_id, f"Neighbor removed: {nid}")
        self.generate_lsa(force=True)
        self.mtree.on_neighbor_gone(nid)

    def listen_overlay(self):
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.tcp_port))
        srv.listen(20)
        log(self.node_id, f"Overlay listening on :{self.tcp_port}")

        while self.running:
            try:
                conn, addr = srv.accept()
                threading.Thread(target=self.handle_inbound, args=(conn, addr), daemon=True).start()
            except Exception:
                break

    def handle_inbound(self, sock, addr):
        buf = ""
        nid = None

        try:
            while nid is None:
                lines, buf = recv_lines(sock, buf)
                if lines is None:
                    return
                for ln in lines:
                    msg = json.loads(ln)
                    if msg.get("type") == "HELLO":
                        nid = msg["node_id"]
                        break

            with self.lock:
                if nid not in self.neighbors_cfg:
                    self.neighbors_cfg[nid] = {
                        "ip": addr[0],
                        "tcp_port": None,
                        "udp_port": None,
                    }
                self.neighbors_in[nid] = sock

            log(self.node_id, f"IN ← {nid}")
            self.mtree.on_new_peer(nid, sock)
            self.peer_recv_loop(nid, sock)

        finally:
            with self.lock:
                if nid in self.neighbors_in and self.neighbors_in[nid] is sock:
                    self.neighbors_in.pop(nid, None)
            close_socket(sock)

    def connect_neighbors_loop(self):
        backoff = {}

        while self.running:
            time.sleep(1)

            with self.lock:
                neighbors = dict(self.neighbors_cfg)
                active_out = set(self.neighbors_out.keys())

            for nid, info in neighbors.items():
                if nid in active_out:
                    continue

                if not info.get("ip") or not info.get("tcp_port"):
                    continue

                backoff.setdefault(nid, 1)

                try:
                    s = socket.socket()
                    s.settimeout(2)
                    s.connect((info["ip"], info["tcp_port"]))
                    s.settimeout(None)

                    send_json(s, {"type": "HELLO", "node_id": self.node_id})

                    with self.lock:
                        self.neighbors_out[nid] = s

                    log(self.node_id, f"OUT → {nid}")
                    backoff[nid] = 1

                    self.mtree.on_new_peer(nid, s)

                    threading.Thread(target=self.peer_recv_loop, args=(nid, s), daemon=True).start()

                except Exception:
                    backoff[nid] = min(backoff[nid] * 2, 10)

    def get_next_hop(self, dst):
        with self.lock:
            entry = self.routes.get(dst)
        return entry[0] if entry else None

    def send_to_node(self, dst, msg):
        if dst == self.node_id:
            return
        
        with self.lock:
            entry = self.routes.get(dst)
            nh = entry[0] if entry else None
            sock = self.neighbors_out.get(nh) if nh else None
        
        if sock:
            send_json(sock, msg)
        else:
            log(self.node_id, f"No route/socket to {dst} via {nh}")

    def peer_recv_loop(self, nid, sock):
        buf = ""
        while self.running:
            lines, buf = recv_lines(sock, buf)
            if lines is None:
                break

            for ln in lines:
                try:
                    msg = json.loads(ln)
                    t = msg.get("type")
                except Exception:
                    continue

                try:
                    if t == "PING":
                        send_json(sock, {"type": "PONG", "ts": msg["ts"]})
                    elif t == "PONG":
                        rtt_raw = time.time() - msg["ts"]
                        self.update_rtt(nid, rtt_raw)
                    elif t == "LSA":
                        self.handle_lsa(msg, from_nid=nid)
                    elif t == "STREAM_ANNOUNCE":
                        self.mtree.handle_stream_announce(msg, from_nid=nid)
                    elif t == "STREAM_JOIN":
                        self.mtree.handle_stream_join(msg, from_nid=nid)
                    elif t == "STREAM_LEAVE":
                        self.mtree.handle_stream_leave(msg, from_nid=nid)
                except Exception as e:
                    log(self.node_id, f"Error handling {t} from {nid}: {e}")

        with self.lock:
            if self.neighbors_out.get(nid) is sock:
                self.neighbors_out.pop(nid, None)
            if self.neighbors_in.get(nid) is sock:
                self.neighbors_in.pop(nid, None)
            self.rtt.pop(nid, None)
        
        close_socket(sock)
        
        self.generate_lsa(force=True)
        self.mtree.on_neighbor_gone(nid)

    def update_rtt(self, nid, rtt_raw):
        changed = False
        with self.lock:
            prev = self.rtt.get(nid)
            if prev is None:
                smoothed = rtt_raw
                changed = True
            else:
                smoothed = RTT_ALPHA * rtt_raw + (1 - RTT_ALPHA) * prev
                if abs(smoothed - prev) > RTT_CHANGE_EPS:
                    changed = True
            self.rtt[nid] = smoothed

        if changed:
            self.generate_lsa()

    def ping_loop(self):
        while self.running:
            time.sleep(PING_INTERVAL)
            now = time.time()
            with self.lock:
                peers = list(self.neighbors_out.items())
            for nid, s in peers:
                send_json(s, {"type": "PING", "ts": now})

    def generate_lsa(self, force=False):
        now = time.time()
        with self.lock:
            if not force and now - self.last_lsa_ts < MIN_LSA_GAP:
                return

            neighbors_costs = dict(self.rtt)
            if not force and neighbors_costs == self.last_lsa_neighbors:
                return

            self.lsa_seq += 1
            self.last_lsa_ts = now
            self.last_lsa_neighbors = neighbors_costs

            lsa = {
                "type": "LSA",
                "origin": self.node_id,
                "seq": self.lsa_seq,
                "neighbors": neighbors_costs,
            }

            self.lsdb[self.node_id] = {
                "seq": self.lsa_seq,
                "neighbors": neighbors_costs,
                "timestamp": now,
            }

        self.flood_lsa(lsa)
        self.compute_routes()

    def handle_lsa(self, lsa, from_nid):
        origin = lsa["origin"]
        seq = lsa["seq"]
        neighs = lsa["neighbors"]
        now = time.time()
        changed = False

        with self.lock:
            old = self.lsdb.get(origin)
            if old is None or seq > old["seq"]:
                self.lsdb[origin] = {
                    "seq": seq,
                    "neighbors": dict(neighs),
                    "timestamp": now,
                }
                changed = True

        if changed:
            self.flood_lsa(lsa, exclude=from_nid)
            self.compute_routes()

    def flood_lsa(self, lsa, exclude=None):
        with self.lock:
            peers = list(self.neighbors_out.items())

        for nid, sock in peers:
            if nid == exclude:
                continue
            send_json(sock, lsa)

    def cleanup_lsdb_loop(self):
        while self.running:
            time.sleep(5)
            now = time.time()
            expired = []
            with self.lock:
                for nid, entry in list(self.lsdb.items()):
                    if nid == self.node_id:
                        continue
                    if now - entry["timestamp"] > MAX_LSA_AGE:
                        expired.append(nid)
                        entry["neighbors"] = {}
                        entry["timestamp"] = now
            if expired:
                self.compute_routes()

    def compute_routes(self):
        with self.lock:
            graph = {nid: dict(entry["neighbors"])
                     for nid, entry in self.lsdb.items()}
            src = self.node_id

        dist = {src: 0.0}
        prev = {src: None}
        pq = [(0.0, src)]

        while pq:
            cost, u = heapq.heappop(pq)
            if cost != dist[u]:
                continue
            for v, w in graph.get(u, {}).items():
                nd = cost + float(w)
                if v not in dist or nd < dist[v]:
                    dist[v] = nd
                    prev[v] = u
                    heapq.heappush(pq, (nd, v))

        routes = {}
        for dst in dist:
            if dst == src:
                continue
            hop = dst
            while prev[hop] is not None and prev[hop] != src:
                hop = prev[hop]
            routes[dst] = (hop, dist[dst])

        with self.lock:
            self.routes = routes

        self.mtree.on_routes_changed()

    def udp_recv_loop(self):
        while self.running:
            try:
                data, addr = self.udp_sock.recvfrom(65535)
                self.mtree.handle_rtp_packet(data, addr)
            except Exception:
                continue

    def source_stream_controller_loop(self):
        sid = "S1"
        while self.running and not self.source_stream_started:
            with self.lock:
                has_neighbors = len(self.neighbors_out) > 0
            
            if has_neighbors:
                if self.video_path:
                    log(self.node_id, f"Source: starting stream {sid}")
                    self.mtree.start_video_stream(sid, self.video_path)
                self.source_stream_started = True
                break
            time.sleep(1.0)

    def run(self):
        threading.Thread(target=self.listen_overlay, daemon=True).start()
        threading.Thread(target=self.udp_recv_loop, daemon=True).start()
        self.register_to_bootstrap()
        threading.Thread(target=self.connect_neighbors_loop, daemon=True).start()
        threading.Thread(target=self.ping_loop, daemon=True).start()
        threading.Thread(target=self.cleanup_lsdb_loop, daemon=True).start()
        threading.Thread(target=self.lsa_loop, daemon=True).start()

        if self.is_source:
            threading.Thread(target=self.source_stream_controller_loop, daemon=True).start()

        log(self.node_id, "Overlay node running.")
        
        if self.client_stream_id is not None:
            gui = ClientGUI(self)
            try:
                gui.run()
            except KeyboardInterrupt:
                pass
            finally:
                self.running = False
                log(self.node_id, "Shutting down after GUI close...")
        else:
            try:
                while self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.running = False
                log(self.node_id, "Shutting down...")

    def lsa_loop(self):
        while self.running:
            time.sleep(LSA_INTERVAL)
            self.generate_lsa()


def node_num_from_id(nid: str) -> int:
    if len(nid) > 1 and nid[0].isalpha():
        return int(nid[1:])
    return int(nid)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print(f"  {sys.argv[0]} server <node_id> <topology.json> [video_path]")
        print(f"  {sys.argv[0]} node   <node_id> <bootstrap_ip>")
        print(f"  {sys.argv[0]} client <node_id> <bootstrap_ip>")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "server":
        if len(sys.argv) not in (4, 5):
            print(f"Usage: {sys.argv[0]} server <node_id> <topology.json> [video_path]")
            sys.exit(1)

        node_id = sys.argv[2]
        topo_file = sys.argv[3]
        video_path = sys.argv[4] if len(sys.argv) == 5 else None

        server = BootstrapServer(topo_file)
        threading.Thread(target=server.run, daemon=True).start()

        n = node_num_from_id(node_id)
        tcp_port = BASE_TCP_PORT + n
        udp_port = BASE_UDP_PORT + n
        bootstrap_ip = "10.0.0.10"

        node = OverlayNode(node_id, tcp_port, udp_port, bootstrap_ip,
                           is_source=True, client_stream_id=None,
                           video_path=video_path)
        node.run()

    elif mode == "node":
        if len(sys.argv) != 4:
            print(f"Usage: {sys.argv[0]} node <node_id> <bootstrap_ip>")
            sys.exit(1)

        node_id = sys.argv[2]
        bootstrap_ip = sys.argv[3]

        n = node_num_from_id(node_id)
        tcp_port = BASE_TCP_PORT + n
        udp_port = BASE_UDP_PORT + n

        node = OverlayNode(node_id, tcp_port, udp_port, bootstrap_ip,
                           is_source=False, client_stream_id=None,
                           video_path=None)
        node.run()

    elif mode == "client":
        if len(sys.argv) != 4:
            print(f"Usage: {sys.argv[0]} client <node_id> <bootstrap_ip>")
            sys.exit(1)

        node_id = sys.argv[2]
        bootstrap_ip = sys.argv[3]

        n = node_num_from_id(node_id)
        tcp_port = BASE_TCP_PORT + n
        udp_port = BASE_UDP_PORT + n

        node = OverlayNode(node_id, tcp_port, udp_port, bootstrap_ip,
                           is_source=False, client_stream_id="S1",
                           video_path=None)
        node.run()

    else:
        print("Unknown mode:", mode)
        sys.exit(1)
