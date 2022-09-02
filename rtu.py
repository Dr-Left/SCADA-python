import socket
import sys
import time
from threading import Thread, Lock

import database_util
import socket_util

rtu_id = 1
database_util.get_tele_data(rtu_id, "yc")


def thread_proc_send(_sock):
    print("rtu thread_proc_send is running!")
    while True:
        records = database_util.get_tele_data(rtu_id, "yc")
        socket_util.send(_sock, "yc", records, lock)
        records = database_util.get_tele_data(rtu_id, "yx")
        socket_util.send(_sock, "yx", records, lock)
        time.sleep(5.0)


def thread_proc_listen(_sock):
    print("rtu thread_proc_listen is running!")
    while True:
        type_, data = socket_util.recv(_sock)
        print(type_)
        if type_ == "ytcmd":
            pass
        elif type_ == "ykcmd":
            pass


if __name__ == "__main__":
    lock = Lock()
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
        print(f"rtu{rtu_id} come to wait accept ...")
        sock, addr = server.accept()
        print(f"rtu{rtu_id} get accept！", sock, addr)
        thread_send = Thread(target=thread_proc_send, args=(sock, ))
        thread_send.start()
        thread_listen = Thread(target=thread_proc_send, args=(sock, ))
        thread_listen.start()
