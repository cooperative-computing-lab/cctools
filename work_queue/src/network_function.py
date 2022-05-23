#! /usr/bin/env python

import socket
import json
import multiprocessing
import os

# Note: To change. the actual name of the module (i.e. instead of
# "coprocess_example") should come from some worker's argument.
import coprocess_example as wq_worker_coprocess


def send_configuration(config):
    config_string = json.dumps(config)
    print(len(config_string) + 1, "\n", config_string, flush=True)

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # modify the port argument to be 0 to listen on an arbitrary port
        s.bind(('localhost', 0))
    except Exception as e:
        s.close()
        print(e)
        exit(1)

    # information to print to stdout for worker
    config = {
            "name": wq_worker_coprocess.name(),
            "port": s.getsockname()[1],
            }

    send_configuration(config)

    while True:
        s.listen()
        conn, addr = s.accept()
        print('Connection from {}'.format(addr))
        while True:
            # peek at message to find newline to get the size
            event_size = None
            line = conn.recv(100, socket.MSG_PEEK)
            eol = line.find(b'\n')
            if eol >= 0:
                size = eol+1
                # actually read the size of the event
                input_spec = conn.recv(size).decode('utf-8').split()
                function_name = input_spec[0]
                event_size = int(input_spec[1])

            if event_size:
                # receive the event itself
                event = conn.recv(event_size)

                event = json.loads(event)
                print('event: {}'.format(event))

                # create a forked process for function handler
                manager = multiprocessing.Manager()
                response = manager.dict()
                p = multiprocessing.Process(target=getattr(wq_worker_coprocess, function_name), args=(event, response))
                p.start()
                p.join()

                response = json.dumps(dict(response))
                response_size = len(response)

                size_msg = "output {}\n".format(response_size)

                # send the size of response
                conn.sendall(size_msg.encode('utf-8'))

                # send response
                conn.sendall(response.encode('utf-8'))

                break

    return 0

if __name__ == "__main__":
    main()


