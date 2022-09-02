import socket
import sys
import time
from multiprocessing import Process
from threading import Thread, Lock

import database_util
import socket_util


def thread_proc_send(_sock, _lock, _rtu_id):
    print("rtu thread_proc_send is running!")
    while True:
        records = database_util.get_rtu_data(_rtu_id, "yc", _lock)
        socket_util.send(_sock, "ycsend", records, _lock)
        records = database_util.get_rtu_data(_rtu_id, "yx", _lock)
        socket_util.send(_sock, "yxsend", records, _lock)
        time.sleep(5.0)


def thread_proc_listen(_sock, _lock, _rtu_id):
    print("rtu thread_proc_listen is running!")
    while True:
        type_, data = socket_util.recv(_sock)
        print(f"rtu{_rtu_id} receive: ", type_, data)
        if type_[-3:] == "cmd":
            database_util.update_rtu_data(_rtu_id, type_, data)
            socket_util.send(_sock, type_[:2] + "ret", None, _lock)
            print(f"rtu{_rtu_id} send: ", type_[:2] + "ret")


def rtu_process(_rtu_id):
    # establishing a connection to database

    # print("rtu_id = ", _rtu_id)
    lock = Lock()
    server_addr = database_util.get_server_addr(f"rtu{_rtu_id}.db", "rtu_info", _rtu_id)
    assert server_addr
    server = socket.socket()
    server.bind(server_addr)
    server.listen(10)

    while True:
        print(f"rtu{_rtu_id} come to wait accept ...")
        sock, addr = server.accept()
        print(f"rtu{_rtu_id} get accept！", sock, addr)
        thread_send = Thread(target=thread_proc_send, args=(sock, lock, _rtu_id))
        thread_send.start()
        thread_listen = Thread(target=thread_proc_listen, args=(sock, lock, _rtu_id))
        thread_listen.start()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        _rtu_id = int(sys.argv[1])
        p = Process(target=rtu_process, args=(_rtu_id,))
        p.start()
    else:
        for _rtu_id in range(1, 7):
            p = Process(target=rtu_process, args=(_rtu_id,))
            p.start()
