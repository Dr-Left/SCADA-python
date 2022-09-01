import socket
import struct
import sys

import database_util

# establishing a connection to database
rtu_id = 1
print("sys.argv = ", sys.argv[1:])
if len(sys.argv) > 1:
    rtu_id = int(sys.argv[1])

print("rtu_id = ", rtu_id)
server_addr = database_util.get_server_addr(rtu_id, f"rtu{rtu_id}.db", "rtu_info")
server = socket.socket()
server.bind(server_addr)
server.listen(10)
conn, addr = server.accept()
while True:
    header = conn.recv(4)
    head_len = struct.unpack("i", header)
    msg = conn.recv(head_len[0]).decode("utf-8")
    print(msg)
    msg = msg.upper()
    conn.send(msg.encode("utf-8"))
