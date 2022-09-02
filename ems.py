import socket
from threading import Thread, Lock
from time import sleep

import database_util
import socket_util


def thread_proc_listen(_sock, _rtu_id):
    print(f"ems thread_proc_listen{_rtu_id} is running!")
    while True:
        type_, data = socket_util.recv(_sock)
        print(f"ems receive from rtu{_rtu_id}: ", type_, data)
        if type_[-4:] == "send":
            database_util.update_ems_data(type_, data)
            socket_util.send(_sock, type_[:2] + "ret", None, lock)
            print(f"ems send to rtu{_rtu_id}: ", type_[:2] + "ret")
            # print(msg)


def thread_proc_query(_sock, type_, data):
    while True:
        print(f"query thread_proc_query{rtu_id} is running!", type_, data)
        socket_util.send(_sock, type_, data, lock)
        sleep(5)


if __name__ == "__main__":
    lock = Lock()
    results = database_util.get_server_addr("ems.db", "ems_rtu_info")
    assert results
    for rtu_id, ip_addr, port in results:
        print(f"come to conn{rtu_id}", rtu_id, ip_addr, port)
        try:
            # TODO: multiprocessing the socket connection
            sock = socket.socket()
            sock.connect((ip_addr, port))
            # "id", "name", "value", "refresh_time", "ctrl_code"
            thread_query = Thread(target=thread_proc_query,
                                  args=(sock, "ykcmd", {"id": 1, "name": f"rtu{rtu_id}.1", "value": 1, "ctrl_code": 1, "refresh_time": 1}))
            thread_query.start()
            thread_listen = Thread(target=thread_proc_listen, args=(sock, rtu_id))
            thread_listen.start()
        except ConnectionRefusedError as err:
            print(err)
