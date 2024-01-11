#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


def wq_network_code():
    import socket
    import json
    import os
    import sys

    # Send a message on an I/O stream by sending the message length and then the (string) message.
    def send_message( stream, data ):
        size = len(data)
        size_msg = "{}\n".format(size)
        stream.sendall(size_msg.encode('utf-8'))
        stream.sendall(data)

    # Receive a standard message from an I/O stream by reading length and then returning the (string) message
    def recv_message( stream ):
        line = stream.readline()
        length = int(line)
        return stream.readall(length).decode('utf-8')

    def remote_execute(func):
        def remote_wrapper(event):
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
            return response
        return remote_wrapper

    read, write = os.pipe()

    def main():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # modify the port argument to be 0 to listen on an arbitrary port
            s.bind(('localhost', 0))
        except Exception as e:
            s.close()
            print(e, file=sys.stderr)
            sys.exit(1)

        # information to print to stdout for worker
        config = {
                "name": name(),  # noqa: F821
                "port": s.getsockname()[1],
                }
        send_message(sys.stdout,json_dumps(config))
        sys.stdout.flush()
        
        abs_working_dir = os.getcwd()
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
                        os.chdir(os.path.join(abs_working_dir, f't.{task_id}'))
                        if exec_method == "direct":
                            response = json.dumps(globals()[function_name](event)).encode("utf-8")
                        else:
                            p = os.fork()
                            if p == 0:
                                response =globals()[function_name](event)
                                os.write(write, json.dumps(response).encode("utf-8"))
                                os._exit(0)
                            elif p < 0:
                                print(f'Network function: unable to fork to execute {function_name}', file=sys.stderr)
                                response = {
                                    "Result": "unable to fork",
                                    "StatusCode": 500
                                }
                                response = json.dumps(response)
                            else:
                                max_read = 65536
                                chunk = os.read(read, max_read)
                                all_chunks = [chunk]
                                while (len(chunk) >= max_read):
                                    chunk = os.read(read, max_read)
                                    all_chunks.append(chunk)
                                response = "".join(all_chunks)
                                os.waitpid(p, 0)
                        send_message(conn,response)
                        break
                except Exception as e:
                    print("Network function encountered exception ", str(e), file=sys.stderr)
                    response = {
                        'Result': f'network function encountered exception {e}',
                        'Status Code': 500
                    }
                    send_message(conn,json_dumps(response))
                finally:
                    os.chdir(abs_working_dir)
        return 0
