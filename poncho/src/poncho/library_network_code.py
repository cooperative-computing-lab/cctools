#!/usr/bin/env python3

# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


# This function serves as the template for Python Library Task.
# A Python Library Task's script will be extracted as the body of this
# function and run on a worker as a pilot task. Upcoming Python
# Function Calls will be executed by this pilot task.
def library_network_code():

    # import relevant libraries.
    import json
    import os
    import sys
    import argparse
    import traceback
    import cloudpickle

    
    # This class captures how results from FunctionCalls are conveyed from 
    # the library to the manager. 
    # For now, all communication details should use this class to generate responses.
    # In the future, this common protocol should be listed someplace else
    # so library tasks from other languages can use.
    class LibraryResponse:
        def __init__(self, result=None, success=None, reason=None):
            self.result = result
            self.success = success
            self.reason = reason

        def generate(self):
            return {'Result': self.result,
                    'Success': self.success,
                    'Reason': self.reason}

    
    # A wrapper around functions in library to extract arguments and formulate responses.
    def remote_execute(func):
        def remote_wrapper(event):
            args = event.get("fn_args", [])
            kwargs = event.get("fn_kwargs", {})
            try:
                result = func(*args, **kwargs)
                success = True
                reason = None
            except Exception as e:  # noqa: F841
                result = None
                success = False
                reason = traceback.format_exc()
            return LibraryResponse(result, success, reason).generate()
        return remote_wrapper

    # Self-identifying message to send back to the worker, including the name of this library.
    def send_configuration(config, out_pipe):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string)}\n{config_string}"
        out_pipe.write(bytes(config_cmd, 'utf-8'))
        out_pipe.flush()

    def main():
        parser = argparse.ArgumentParser('Parse input and output file descriptors this process should use. The relevant fds should already be prepared by the vine_worker.')
        parser.add_argument('--input-fd', required=True, type=int, help='input fd to receive messages from the vine_worker via a pipe')
        parser.add_argument('--output-fd', required=True, type=int, help='output fd to send messages to the vine_worker via a pipe')
        args = parser.parse_args()

        # Open communication pipes to vine_worker.
        # The file descriptors are inherited from the vine_worker parent process 
        # and should already be open for reads and writes.
        # Below lines only convert file descriptors into native Python file objects.
        in_pipe = os.fdopen(args.input_fd, 'rb')
        out_pipe = os.fdopen(args.output_fd, 'wb')

        config = {
                "name": name(),  # noqa: F821
        }
        send_configuration(config, out_pipe)
        
        # A pair of pipes to communicate with the child process if needed.
        read, write = os.pipe()

        while True:
            while True:
                # wait for message from worker about what function to execute
                try:
                    # get length of first buffer
                    # remove trailing \n
                    buffer_len = int(in_pipe.readline()[:-1])
                # if the worker closed the pipe connected to the input of this process, we should just exit
                # stderr is already dup2'ed to send error messages to an output file that can be brought back for further analysis.
                except Exception as e:
                    print("Cannot read message from the manager, exiting. ", e, file=sys.stderr)
                    sys.exit(1)

                # read first buffer, this buffer should contain only utf-8 chars.
                line = str(in_pipe.read(buffer_len), encoding='utf-8')
                function_name, event_size, function_sandbox = line.split(" ", maxsplit=2)
                
                if event_size:
                    event_size = int(event_size)
                    event_str = in_pipe.read(event_size)

                    # load the event into a Python object
                    event = cloudpickle.loads(event_str)

                    # see if the user specified an execution method
                    exec_method = event.get("remote_task_exec_method", None)

                    # library either directly executes or forks to do so.
                    if exec_method == "direct":
                        library_sandbox = os.getcwd()
                        try:
                            os.chdir(function_sandbox)
                            response = cloudpickle.dumps(globals()[function_name](event))
                        except Exception as e:
                            print(f'Library code: Function call failed due to {e}', file=sys.stderr)
                            sys.exit(1)
                        finally:
                            os.chdir(library_sandbox)
                    else:
                        p = os.fork()

                        # child executes and pipes result back to parent.
                        if p == 0:
                            os.chdir(function_sandbox)
                            response = cloudpickle.dumps(globals()[function_name](event))
                            written = 0
                            buff = len(response).to_bytes(8, sys.byteorder)+response
                            while written < len(buff):
                                written += os.write(write, buff[written:])
                            os._exit(0)
                        elif p < 0:
                            print(f'Library code: unable to fork to execute {function_name}', file=sys.stderr)
                            result = None
                            success = False
                            reason = f'unable to fork-exec function {function_name}'
                            response = LibraryResponse(result, success, reason).generate()

                        # parent collects result and waits for child to exit.
                        else:
                            response_len = b''
                            while len(response_len) < 8:
                                response_len += os.read(read, 8-len(response_len))
                            response_len = int.from_bytes(response_len, sys.byteorder)
                            response = b''
                            while len(response) < response_len:
                                response += os.read(read, response_len-len(response))
                            os.waitpid(p, 0)
                    
                    out_pipe.write(bytes(str(len(response)), 'utf-8')+b'\n'+response)
                    out_pipe.flush()
        return 0
