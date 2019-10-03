# to run example:
# in some terminal: python3 example_udp_server.py
# in another terminal: python3 example_client_with_decorator.py

import socket
import pickle

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.bind(('localhost', 9800))

while True:
    data, addr = sock.recvfrom(1024)
    print("message: ", pickle.loads(data))
