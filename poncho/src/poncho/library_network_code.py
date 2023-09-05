#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


def library_network_code():
    import json
    import os
    import sys
    import argparse

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

    def send_configuration(config, out_pipe):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string)}\n{config_string}"
        out_pipe.write(config_cmd)
        out_pipe.flush()

    def main():
        parser = argparse.ArgumentParser('Parse input and output file descriptors this process should use. The relevant fds should already be prepared by the vine_worker.')
        parser.add_argument('--input-fd', required=True, type=int, help='input fd to receive messages from the vine_worker via a pipe')
        parser.add_argument('--output-fd', required=True, type=int, help='output fd to send messages to the vine_worker via a pipe')
        args = parser.parse_args()

        # Open communication pipes to vine_worker.
        # The file descriptors should already be open for reads and writes.
        # Below lines only convert file descriptors into native Python file objects.
        in_pipe = os.fdopen(args.input_fd, 'r')
        out_pipe = os.fdopen(args.output_fd, 'w')

        config = {
            "name": name(),
        }
        send_configuration(config, out_pipe)

        while True:
            while True:
                # wait for message from worker about what function to execute
                try:
                    # get length of first buffer
                    # remove trailing \n
                    buffer_len = int(in_pipe.readline()[:-1])
                # if the worker closed the pipe connected to the input of this process, we should just exit
                except Exception as e:
                    print("Cannot read message from the manager, exiting. ", e, file=sys.stderr)
                    sys.exit(1)

                # read first buffer
                line = in_pipe.read(buffer_len)

                function_name, event_size, function_sandbox = line.split(" ", maxsplit=2)
                if event_size:
                    # receive the bytes containing the event and turn it into a string
                    event_str = in_pipe.readline()[:-1]
                    if len(event_str) != int(event_size):
                        print(event_str, len(event_str), event_size, file=sys.stderr)
                        print("Size of event does not match what was sent: exiting", file=sys.stderr)
                        sys.exit(1)

                    # turn the event into a python dictionary
                    event = json.loads(event_str)

                    # see if the user specified an execution method
                    exec_method = event.get("remote_task_exec_method", None)

                    if exec_method == "direct":
                        library_sandbox = os.getcwd()
                        try:
                            os.chdir(function_sandbox)
                            response = json.dumps(globals()[function_name](event))
                        except Exception as e:
                            print(f'Library code: Function call failed due to {e}', file=sys.stderr)
                            sys.exit(1)
                        finally:
                            os.chdir(library_sandbox)
                    else:
                        p = os.fork()
                        if p == 0:
                            os.chdir(function_sandbox)
                            response = globals()[function_name](event)
                            os.write(write, json.dumps(response).encode("utf-8"))
                            os._exit(0)
                        elif p < 0:
                            print(f'Library code: unable to fork to execute {function_name}', file=sys.stderr)
                            response = {
                                "Result": "unable to fork",
                                "StatusCode": 500
                            }
                        else:
                            max_read = 65536
                            chunk = os.read(read, max_read).decode("utf-8")
                            all_chunks = [chunk]
                            while (len(chunk) >= max_read):
                                chunk = os.read(read, max_read).decode("utf-8")
                                all_chunks.append(chunk)
                            response = "".join(all_chunks)
                            os.waitpid(p, 0)
                    out_pipe.write(response+'\n')
                    out_pipe.flush()
        return 0
