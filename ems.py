import random
import socket
import time
from queue import Queue
from threading import Thread, Lock
from time import sleep

import database_util
import socket_util


def thread_proc_senddata(_sock, queue_send):
    while True:
        data = queue_send.get()
        socket_util.send(_sock, data[0], data[1])


def thread_proc_listen(_sock, _rtu_id, queue_send):
    print(f"ems thread_proc_listen{_rtu_id} is running!")
    while True:
        type_, data = socket_util.recv(_sock)
        print(f"ems receive from rtu{_rtu_id}: ", type_, data)
        if type_[-4:] == "send":
            database_util.update_ems_ycyx_data(type_, data)
            queue_send.put((type_[:2]+"ret", None))
            # socket_util.send(_sock, type_[:2] + "ret", None)  # no lock, use queue technique
            print(f"ems send to rtu{_rtu_id}: ", type_[:2] + "ret")
            # print(msg)


def thread_proc_query(_sock, type_, data, queue_send):
    while True:
        for pnt_no in range(1, 6+1):
            data["value"] = random.randint(1, 10)
            data["id"] = pnt_no
            print(f"query thread_proc_query{rtu_id} is running!", type_, data)
            queue_send.put((type_, data))
            # socket_util.send(_sock, type_, data)  # no lock, use queue technique
            database_util.update_ems_ykyt_data(type_, data)
            sleep(2)


if __name__ == "__main__":
    lock = Lock()
    queue_send = Queue(1024)
    results = database_util.get_server_addr("ems.db", "ems_rtu_info")
    assert results
    for rtu_id, ip_addr, port in results:
        print(f"come to conn{rtu_id}", rtu_id, ip_addr, port)
        try:
            # TODO: multiprocessing the socket connection
            sock = socket.socket()
            sock.connect((ip_addr, port))
            # "id", "name", "value", "refresh_time", "ctrl_code"
            thread_senddata = Thread(target=thread_proc_senddata,
                                     args=(sock, queue_send))
            thread_query = Thread(target=thread_proc_query,
                                  args=(sock, "ykcmd", {"id": 1, "name": f"rtu{rtu_id}.1", "value": 1, "ctrl_code": 1,
                                                        "refresh_time": int(time.time())}, queue_send))
            thread_query.start()
            thread_query1 = Thread(target=thread_proc_query,
                                  args=(sock, "ytcmd", {"id": 1, "name": f"rtu{rtu_id}.1", "value": 2.0, "ctrl_code": 1,
                                                        "refresh_time": int(time.time())}, queue_send))
            thread_query1.start()
            thread_listen = Thread(target=thread_proc_listen, args=(sock, rtu_id, queue_send))
            thread_listen.start()
        except ConnectionRefusedError as err:
            print(err)
