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
    import select
    import signal
    from threadpoolctl import threadpool_limits

    # self-pipe to turn a sigchld signal when a child finishes execution
    # into an I/O event.
    r, w = os.pipe()

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

            # in case of FutureFunctionCall tasks
            new_args = []
            for arg in args:
                if isinstance(arg, dict) and "VineFutureFile" in arg:
                    with open(arg["VineFutureFile"], 'rb') as f:
                        output = cloudpickle.load(f)["Result"]
                        new_args.append(output)
                else:
                    new_args.append(arg)
            args = tuple(new_args)

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
    # Send back a SIGCHLD to interrupt worker sleep and get it to work.
    def send_configuration(config, out_pipe_fd, worker_pid):
        config_string = json.dumps(config)
        config_cmd = f"{len(config_string)}\n{config_string}"
        os.writev(out_pipe_fd, [bytes(config_cmd, 'utf-8')])
        os.kill(worker_pid, signal.SIGCHLD)

    # Handler to sigchld when child exits.
    def sigchld_handler(signum, frame):
        # write any byte to signal that there's at least 1 child
        os.writev(w, [b'a'])

    # Read data from worker, start function, and dump result to `outfile`.
    def start_function(in_pipe_fd, thread_limit=1):
        # read length of buffer to read
        buffer_len = b''
        while True:
            c = os.read(in_pipe_fd, 1)
            if c == b'':
                print('Library code: cant get length', file=sys.stderr)
                exit(1)
            elif c == b'\n':
                break
            else:
                buffer_len += c
        buffer_len = int(buffer_len)

        # now read the buffer to get invocation details
        line = str(os.read(in_pipe_fd, buffer_len), encoding='utf-8')
        function_id, function_name, function_sandbox, function_stdout_file = line.split(" ", maxsplit=3)
        function_id = int(function_id)

        if not function_name:
            # malformed message from worker so we exit
            print('malformed message from worker. Exiting..', file=sys.stderr)
            exit(1)
        
        with threadpool_limits(limits=thread_limit):
            # exec method for now is fork only, direct will be supported later
            exec_method = 'fork'
            if exec_method == "direct":
                library_sandbox = os.getcwd()
                try:
                    os.chdir(function_sandbox)

                    # parameters are represented as infile.
                    os.chdir(function_sandbox)
                    with open('infile', 'rb') as f:
                        event = cloudpickle.load(f)

                    # output of execution should be dumped to outfile.
                    with open('outfile', 'wb') as f:
                        cloudpickle.dump(globals()[function_name](event), f)

                except Exception as e:
                    print(f'Library code: Function call failed due to {e}', file=sys.stderr)
                    sys.exit(1)
                finally:
                    os.chdir(library_sandbox)
            else:
                p = os.fork()
                if p == 0:
                    # setup stdout/err for a function call so we can capture them.
                    stdout_capture_fd = os.open(function_stdout_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                    os.dup2(stdout_capture_fd, sys.stdout.fileno())
                    os.dup2(stdout_capture_fd, sys.stderr.fileno())

                    # parameters are represented as infile.
                    os.chdir(function_sandbox)
                    with open('infile', 'rb') as f:
                        event = cloudpickle.load(f)

                    # output of execution should be dumped to outfile.
                    with open('outfile', 'wb') as f:
                        cloudpickle.dump(globals()[function_name](event), f)
                    os.close(stdout_capture_fd)
                    os._exit(0)
                elif p < 0:
                    print(f'Library code: unable to fork to execute {function_name}', file=sys.stderr)
                    return -1

                # return pid and function id of child process to parent.
                else:
                    return p, function_id

        return -1

    # Send result of a function execution to worker. Wake worker up to do work with SIGCHLD.
    def send_result(out_pipe_fd, task_id, worker_pid):
        buff = bytes(str(task_id), 'utf-8')
        buff = bytes(str(len(buff)), "utf-8") + b"\n" + buff
        os.writev(out_pipe_fd, [buff])
        os.kill(worker_pid, signal.SIGCHLD)

    def main():
        ppid = os.getppid()
        parser = argparse.ArgumentParser('Parse input and output file descriptors this process should use. The relevant fds should already be prepared by the vine_worker.')
        parser.add_argument('--input-fd', required=True, type=int, help='input fd to receive messages from the vine_worker via a pipe')
        parser.add_argument('--output-fd', required=True, type=int, help='output fd to send messages to the vine_worker via a pipe')
        parser.add_argument('--task-id', required=False, type=int, default=-1, help='task id for this library.')
        parser.add_argument('--library-cores', required=False, type=int, default=1, help='number of cores of this library')
        parser.add_argument('--function-slots', required=False, type=int, default=1, help='number of function slots of this library')
        parser.add_argument('--worker-pid', required=True, type=int, help='pid of main vine worker to send sigchild to let it know theres some result.')
        args = parser.parse_args()

        # check if library cores and function slots are valid
        if args.function_slots > args.library_cores:
            print('Library code: function slots cannot be more than library cores', file=sys.stderr)
            exit(1)
        elif args.function_slots < 1:
            print('Library code: function slots cannot be less than 1', file=sys.stderr)
            exit(1)
        elif args.library_cores < 1:
            print('Library code: library cores cannot be less than 1', file=sys.stderr)
            exit(1)
        thread_limit = args.library_cores // args.function_slots

        # Open communication pipes to vine_worker.
        # The file descriptors are inherited from the vine_worker parent process
        # and should already be open for reads and writes.
        in_pipe_fd = args.input_fd
        out_pipe_fd = args.output_fd

        # send configuration of library, just its name for now
        config = {
            "name": name(),  # noqa: F821
        }
        send_configuration(config, out_pipe_fd, args.worker_pid)

        # mapping of child pid to function id of currently running functions
        pid_to_func_id = {}

        # register sigchld handler to turn a sigchld signal into an I/O event
        signal.signal(signal.SIGCHLD, sigchld_handler)

        # 5 seconds to wait for select, any value long enough would probably do
        timeout = 5

        while True:
            # check if parent exits
            c_ppid = os.getppid()
            if c_ppid != ppid or c_ppid == 1:
                exit(0)

            # wait for messages from worker or child to return
            rlist, wlist, xlist = select.select([in_pipe_fd, r], [], [], timeout)

            for re in rlist:
                # worker has a function, run it
                if re == in_pipe_fd:
                    pid, func_id = start_function(in_pipe_fd, thread_limit)
                    pid_to_func_id[pid] = func_id
                else:
                    # at least 1 child exits, reap all.
                    # read only once as os.read is blocking if there's nothing to read.
                    # note that there might still be bytes in `r` but it's ok as they will
                    # be discarded in the next iterations.
                    os.read(r, 1)
                    while len(pid_to_func_id) > 0:
                        c_pid, c_exit_status = os.waitpid(-1, os.WNOHANG)
                        if c_pid > 0:
                            send_result(out_pipe_fd, pid_to_func_id[c_pid], args.worker_pid)
                            del pid_to_func_id[c_pid]
                        # no exited child to reap, break
                        else:
                            break
        return 0
