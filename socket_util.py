import json
import struct


def send(sock, _type, data, lock) -> None:
    send_data = {"type": _type, "data": data}
    body = json.dumps(send_data).encode("utf-8")  # body is a json string which represents a dict of type & data
    header = struct.pack("i", len(body))
    # TODO: lock    print("send:", header, body)

    lock.acquire()
    sock.send(header)
    sock.send(body)
    lock.release()


def recv(sock) -> tuple[str, dict]:
    """

    :param sock:
    :return: a tuple of (type, data), where data is a dict, no json anymore
    """
    header = sock.recv(4)
    head_len = struct.unpack("i", header)
    msg = None
    try:
        bytes = sock.recv(head_len[0])
        msg = bytes.decode("utf-8")
        body = json.loads(msg)  # body now is a dict of type & data
    except Exception as err:
        print("Json parsing error! Bad data:", err)
        print("Error message: ", header, bytes)
    print("receive", header, msg)

    return body["type"], body["data"]
