import socket
import struct

import database_util

# import json


server_addr = database_util.get_server_addr(1, "ems.db", "ems_rtu_info")
assert server_addr is not None
client = socket.socket()
client.connect(server_addr)
while True:
    msg = input("Please enter the message: >>").strip()
    header = struct.pack("i", len(msg))
    client.send(header)
    client.send(msg.encode("utf-8"))
    recv = client.recv(1024)
    print(recv.decode("utf-8"))
