#! /usr/bin/env python3

import sys, json
import socket
import json
import os
import sys
import threading
import queue

import socket
import json
import os
import sys
import threading
import queue
def remote_execute(func):
    def remote_wrapper(event, q=None):
        if q:
            event = json.loads(event)
        kwargs = event["fn_kwargs"]
        args = event["fn_args"]
        try:
            response = {
                "Result": func(*args, **kwargs),
                "StatusCode": 200
            }
        except Exception as e:
            response = { 
                "Result": str(e),
                "StatusCode": 500 
            }
        if not q:
            return response
        q.put(response)
    return remote_wrapper
    
read, write = os.pipe() 
def send_configuration(config):
    config_string = json.dumps(config)
    config_cmd = f"{len(config_string) + 1}\n{config_string}\n"
    sys.stdout.write(config_cmd)
    sys.stdout.flush()
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
            "name": name(),
            "port": s.getsockname()[1],
            }
    send_configuration(config)
    while True:
        s.listen()
        conn, addr = s.accept()
        print('Network function: connection from {}'.format(addr), file=sys.stderr)
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
                task_id = int(input_spec[1])
                event_size = int(input_spec[2])
            try:
                if event_size:
                    # receive the bytes containing the event and turn it into a string
                    event_str = conn.recv(event_size).decode("utf-8")
                    # turn the event into a python dictionary
                    event = json.loads(event_str)
                    # see if the user specified an execution method
                    exec_method = event.get("remote_task_exec_method", None)
                    print('Network function: recieved event: {}'.format(event), file=sys.stderr)
                    os.chdir(f"t.{task_id}")
                    if exec_method == "thread":
                        # create a forked process for function handler
                        q = queue.Queue()
                        p = threading.Thread(target=globals()[function_name], args=(event_str, q))
                        p.start()
                        p.join()
                        response = json.dumps(q.get()).encode("utf-8")
                    elif exec_method == "direct":
                        response = json.dumps(globals()[function_name](event)).encode("utf-8")
                    else:
                        p = os.fork()
                        if p == 0:
                            response =globals()[function_name](event)
                            os.write(write, json.dumps(response).encode("utf-8"))
                            os._exit(-1)
                        elif p < 0:
                            print('Network function: unable to fork', file=sys.stderr)
                            response = { 
                                "Result": "unable to fork",
                                "StatusCode": 500 
                            }
                        else:
                            chunk = os.read(read, 65536).decode("utf-8")
                            all_chunks = [chunk]
                            while (len(chunk) >= 65536):
                                chunk = os.read(read, 65536).decode("utf-8")
                                all_chunks.append(chunk)
                            response = "".join(all_chunks).encode("utf-8")
                            os.waitid(os.P_PID, p, os.WEXITED)
                    response_size = len(response)
                    size_msg = "{}\n".format(response_size)
                    # send the size of response
                    conn.sendall(size_msg.encode('utf-8'))
                    # send response
                    conn.sendall(response)
                    os.chdir("..")
                    break
            except Exception as e:
                print("Network function encountered exception ", str(e), file=sys.stderr)
    return 0
def name():
    return 'my_coprocess'
@remote_execute
def add(x, y):
    return x + y
@remote_execute
def multiply(x, y):
    return x * y
@remote_execute
def kwargs_test(x=5, y=6, z=7):
    return x + y * z
@remote_execute
def no_arguments_test(a, b, c):
    return a + b + c
@remote_execute
def exception_test():
    raise Exception('I will raise an exception')
if __name__ == "__main__":
	main()

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
