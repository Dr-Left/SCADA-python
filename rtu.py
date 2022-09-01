import json
import socket
import sys
import time
from threading import Thread

import database_util
import socket_util

rtu_id = 1
database_util.get_tele_data(rtu_id, "yc")


def thread_proc(conn, addr):
    while True:
        records = database_util.get_tele_data(rtu_id, "yc")
        msg = json.dumps(records)
        socket_util.send(conn, msg)

        records = database_util.get_tele_data(rtu_id, "yx")
        msg = json.dumps(records)
        socket_util.send(conn, msg)

        time.sleep(5.0)


if __name__ == "__main__":
    # establishing a connection to database
    print("sys.argv = ", sys.argv[1:])
    if len(sys.argv) > 1:
        rtu_id = int(sys.argv[1])

    print("rtu_id = ", rtu_id)
    server_addr = database_util.get_server_addr(f"rtu{rtu_id}.db", "rtu_info", rtu_id)
    assert server_addr
    server = socket.socket()
    server.bind(server_addr)
    server.listen(10)

    while True:
        print("come to wait accept ...")
        conn, addr = server.accept()
        print("get accept", conn, addr)
        t1 = Thread(target=thread_proc, args=(conn, addr))
        t1.start()
        # msg = conn.recv(head_len[0]).decode("utf-8")
        # header = conn.recv(4)
        # head_len = struct.unpack("i", header)

        # print(msg)
        # msg = msg.upper()
        # conn.send(msg.encode("utf-8"))
