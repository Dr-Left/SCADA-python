import socket
from threading import Thread, Lock


import database_util
import socket_util


def thread_proc_listen(_sock, _rtu_id):
    print("ems thread_proc_listen is running!")
    while True:
        type_, data = socket_util.recv(_sock)
        database_util.update_tele_data(data)  # TODO: add a type identifying process in the database_util
        # print(msg)


def thread_proc_query(_sock, type_, data):
    print("query thread_proc_query is running!")
    socket_util.send(_sock, type_, data, lock)


if __name__ == "__main__":
    lock = Lock()
    results = database_util.get_server_addr("ems.db", "ems_rtu_info")
    assert results
    for rtu_id, ip_addr, port in results:
        print("come to conn", rtu_id, ip_addr, port)
        try:
            sock = socket.socket()
            sock.connect((ip_addr, port))
            thread_query = Thread(target=thread_proc_query, args=(sock, "ykcmd", {"Voltage": 100}))
            thread_query.start()
            thread_listen = Thread(target=thread_proc_listen, args=(sock, rtu_id))
            thread_listen.start()
        except ConnectionRefusedError as err:
            print(err)
