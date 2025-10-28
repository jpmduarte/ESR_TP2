import socket

HOST = "0.0.0.0"
PORT = 6100

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((HOST, PORT))

print(f"Servidor UDP ouvindo em {HOST}:{PORT}")

while True:
    data, addr = sock.recvfrom(1024)
    print(f"Recebido de {addr}: {data.decode()}")
    reply = f"ACK: {data.decode()}"
    sock.sendto(reply.encode(), addr)
