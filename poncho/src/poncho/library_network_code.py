#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


def library_network_code():
    import json
    import os
    import sys

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

    def send_configuration(config):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string) + 1}\n{config_string}\n"
        sys.stdout.write(config_cmd)
        sys.stdout.flush()

    def main():
        config = {
            "name": name(),
        }
        send_configuration(config)
        while True:
            while True:
                # wait for message from worker about what function to execute
                try:
                    line = input()
                # if the worker closed the pipe connected to the input of this process, we should just exit
                except EOFError:
                    sys.exit(0)
                function_name, event_size, function_sandbox = line.split(" ", maxsplit=2)
                if event_size:
                    # receive the bytes containing the event and turn it into a string
                    event_str = input()
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
                    print(response, flush=True)
        return 0
