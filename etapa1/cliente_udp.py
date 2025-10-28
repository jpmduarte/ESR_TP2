import socket

SERVER_IP = "10.0.0.1"   # ajuste conforme a topologia
SERVER_PORT = 6100

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

for i in range(5):
    msg = f"Mensagem {i}"
    sock.sendto(msg.encode(), (SERVER_IP, SERVER_PORT))
    data, addr = sock.recvfrom(1024)
    print("Resposta:", data.decode())

sock.close()
