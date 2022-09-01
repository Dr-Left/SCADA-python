import json
import socket
from threading import Thread

import database_util
import socket_util


def thread_proc(rtu_id, ip_addr, port):
    try:
        print("come to conn", rtu_id, ip_addr, port)
        sock = socket.socket()
        sock.connect((ip_addr, port))
        while True:
            recv = socket_util.recv(sock)
            try:
                msg = json.loads(recv)  # a list of dict
            except Exception as err:
                print("bad data!")
                print(recv)
                print(err)
            database_util.update_tele_data(msg)
            # print(msg)

    except Exception as err:
        print(err)


if __name__ == "__main__":
    results = database_util.get_server_addr("ems.db", "ems_rtu_info")
    assert results
    for rtu_id, ip_addr, port in results:
        thread = Thread(target=thread_proc, args=(rtu_id, ip_addr, port))
        thread.start()
    # client = socket.socket()
    # client.connect(server_addr)
    # while True:
    #     msg = input("Please enter the message: >>").strip()
    #     header = struct.pack("i", len(msg))
    #     client.send(header)
    #     client.send(msg.encode("utf-8"))
    #     recv = client.recv(1024)
    #     print(recv.decode("utf-8"))
