import socket

client = socket.socket()
client.connect(("127.0.0.1", 7700))
while True:
    msg = input("Please enter the message: >>").strip()
    client.send(msg.encode("utf-8"))
    recv = client.recv(1024)
    print(recv.decode("utf-8"))
