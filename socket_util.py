import struct


def send(conn, msg):
    header = struct.pack("i", len(msg))
    # print(len(msg))
    conn.send(header)
    # print(msg)
    conn.send(msg.encode("utf-8"))


def recv(sock):
    header = sock.recv(4)
    head_len = struct.unpack("i", header)
    msg = sock.recv(head_len[0]).decode("utf-8")
    # print(msg)
    return msg
