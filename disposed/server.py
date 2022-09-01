import socket

server = socket.socket()
server.bind(("127.0.0.1", 7700))
server.listen(10)
conn, addr = server.accept()
while True:
    data = conn.recv(1024)
    msg = data.decode("utf-8")
    print(msg)
    msg = msg.upper()
    conn.send(msg.encode("utf-8"))
