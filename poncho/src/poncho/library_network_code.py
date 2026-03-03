# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


# This file serves as the template for Python Library Task.
# A Python Library Task runs on a worker as a pilot task. Upcoming Python
# Function Calls will be executed by this pilot task.

# import relevant libraries.
import json
import os
import fcntl
import sys
import argparse
import traceback
import cloudpickle
import select
import signal
import time
from datetime import datetime
import socket
from threadpoolctl import threadpool_limits
from ndcctools.taskvine.utils import load_variable_from_library

# self-pipe to turn a sigchld signal when a child finishes execution
# into an I/O event.
r, w = os.pipe()
exec_method = None

# infile load mode for function tasks inside this library
function_infile_load_mode = None


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
        return {
            "Result": self.result,
            "Success": self.success,
            "Reason": self.reason,
        }


# A wrapper around functions in library to extract arguments and formulate responses.
def remote_execute(func):
    def remote_wrapper(event):
        args = event.get("fn_args", [])
        kwargs = event.get("fn_kwargs", {})

        # in case of FutureFunctionCall tasks
        new_args = []
        for arg in args:
            if isinstance(arg, dict) and "VineFutureFile" in arg:
                with open(arg["VineFutureFile"], "rb") as f:
                    output = cloudpickle.load(f)["Result"]
                    new_args.append(output)
            else:
                new_args.append(arg)
        args = tuple(new_args)

        try:
            result = func(*args, **kwargs)
            success = True
            reason = None
        except Exception:
            result = None
            success = False
            reason = traceback.format_exc()
        return LibraryResponse(result, success, reason).generate()

    return remote_wrapper


# Handler to sigchld when child exits.
def sigchld_handler(signum, frame):
    # write any byte to signal that there's at least 1 child
    # if this fails, the notification mechanism has failed and the error will be raised normally
    os.write(w, b"a")


# Load the infile for a function task inside this library
def load_function_infile(in_file_path):
    if function_infile_load_mode == "cloudpickle":
        with open(in_file_path, "rb") as f:
            return cloudpickle.load(f)
    elif function_infile_load_mode == "json":
        with open(in_file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        raise ValueError(f"invalid infile load mode: {function_infile_load_mode}")


# Read data from worker, start function, and dump result to `outfile`.
def start_function(in_pipe_fd, thread_limit=1):
    # read length of buffer to read
    buffer_len = b""
    while True:
        c = os.read(in_pipe_fd, 1)
        if c == b"":
            stdout_timed_message(f"can't get length from in_pipe_fd {in_pipe_fd}")
            exit(1)
        elif c == b"\n":
            break
        else:
            buffer_len += c
    buffer_len = int(buffer_len)
    # now read the buffer to get invocation details
    line = str(os.read(in_pipe_fd, buffer_len), encoding="utf-8")

    try:
        (
            function_id,
            function_name,
            function_sandbox,
            function_stdout_filename
        ) = line.split(" ", maxsplit=3)
    except Exception as e:
        stdout_timed_message(f"error: not enough values to unpack from {line} (expected 4 items), exception: {e}")
        exit(1)

    try:
        function_id = int(function_id)
    except Exception as e:
        stdout_timed_message(f"error: can't turn {function_id} into an integer, exception: {e}")
        exit(1)

    if not function_name:
        # malformed message from worker so we exit
        stdout_timed_message(f"error: invalid function name, malformed message {line} from worker")
        exit(1)

    with threadpool_limits(limits=thread_limit):
        if exec_method == "direct":
            library_sandbox = os.getcwd()
            try:
                os.chdir(function_sandbox)

                # parameters are represented as infile.
                event = load_function_infile("infile")

                # output of execution should be dumped to outfile.
                result = globals()[function_name](event)
                try:
                    with open("outfile", "wb") as f:
                        cloudpickle.dump(result, f)
                except Exception:
                    if os.path.exists("outfile"):
                        os.remove("outfile")
                    raise

                try:
                    if not result["Success"]:
                        raise Exception(result["Reason"])
                except Exception:
                    raise

            except Exception as e:
                stdout_timed_message(f"Library code: Function call failed due to {e}")
                sys.exit(1)
            finally:
                os.chdir(library_sandbox)
            return -1, function_id
        elif exec_method == "fork":
            try:
                infile_path = os.path.join(function_sandbox, "infile")
                event = load_function_infile(infile_path)
            except Exception:
                stdout_timed_message(f"TASK {function_id} error: can't load the arguments from {infile_path}")
                return -1, function_id
            p = os.fork()
            if p == 0:
                exit_status = 1

                try:
                    # change the working directory to the function's sandbox
                    os.chdir(function_sandbox)

                    stdout_timed_message(f"TASK {function_id} {function_name} arrives, starting to run in process {os.getpid()}")

                    try:
                        # each child process independently redirects its own stdout/stderr.
                        with open(function_stdout_filename, "wb") as f:
                            os.dup2(f.fileno(), sys.stdout.fileno())  # redirect stdout
                            os.dup2(f.fileno(), sys.stderr.fileno())  # redirect stderr

                        # once the file descriptors are redirected, we can start the function execution
                        stdout_timed_message(f"TASK {function_id} {function_name} starts in PID {os.getpid()}")
                        result = globals()[function_name](event)
                        stdout_timed_message(f"TASK {function_id} {function_name} finished")

                    except Exception:
                        stdout_timed_message(f"TASK {function_id} error: can't execute {function_name} due to {traceback.format_exc()}")
                        exit_status = 2
                        raise

                    try:
                        with open("outfile", "wb") as f:
                            cloudpickle.dump(result, f)
                    except Exception:
                        stdout_timed_message(f"TASK {function_id} error: can't load the result from outfile")
                        exit_status = 3
                        if os.path.exists("outfile"):
                            os.remove("outfile")
                        raise

                    try:
                        if not result["Success"]:
                            exit_status = 4
                    except Exception:
                        stdout_timed_message(f"TASK {function_id} error: the result is invalid")
                        exit_status = 5
                        raise

                    # nothing failed
                    stdout_timed_message(f"TASK {function_id} finished successfully")
                    exit_status = 0
                except Exception as e:
                    stdout_timed_message(f"TASK {function_id} error: execution failed due to {e}")
                finally:
                    os._exit(exit_status)
            elif p < 0:
                stdout_timed_message(f"TASK {function_id} error: unable to fork to execute {function_name}")
                return -1, function_id

            # return pid and function id of child process to parent.
            else:
                return p, function_id
        else:
            stdout_timed_message(f"error: invalid execution method {exec_method}")
            return -1, function_id


# Send result of a function execution to worker. Wake worker up to do work with SIGCHLD.
def send_result(out_pipe_fd, worker_pid, task_id, exit_code):
    buff = bytes(f"{task_id} {exit_code}", "utf-8")
    buff = bytes(str(len(buff)), "utf-8") + b"\n" + buff
    os.writev(out_pipe_fd, [buff])
    os.kill(worker_pid, signal.SIGCHLD)


# Self-identifying message to send back to the worker, including the name of this library.
# Send back a SIGCHLD to interrupt worker sleep and get it to work.
def send_configuration(config, out_pipe_fd, worker_pid):
    config_string = json.dumps(config)
    config_cmd = f"{len(config_string)}\n{config_string}"
    os.writev(out_pipe_fd, [bytes(config_cmd, "utf-8")])
    os.kill(worker_pid, signal.SIGCHLD)


# Use os.write to stdout instead of print for multi-processing safety
def stdout_timed_message(message):
    timestamp = datetime.now().strftime("%m/%d/%y %H:%M:%S.%f")
    os.write(sys.stdout.fileno(), f"{timestamp} {message}\n".encode())


def main():
    ppid = os.getppid()

    parser = argparse.ArgumentParser(
        "Parse input and output file descriptors this process should use. The relevant fds should already be prepared by the vine_worker."
    )
    parser.add_argument(
        "--in-pipe-fd",
        required=True,
        type=int,
        help="input fd to receive messages from the vine_worker via a pipe",
    )
    parser.add_argument(
        "--out-pipe-fd",
        required=True,
        type=int,
        help="output fd to send messages to the vine_worker via a pipe",
    )
    parser.add_argument(
        "--task-id",
        required=False,
        type=int,
        default=-1,
        help="task id for this library.",
    )
    parser.add_argument(
        "--library-cores",
        required=False,
        type=int,
        default=1,
        help="number of cores of this library",
    )
    parser.add_argument(
        "--function-slots",
        required=False,
        type=int,
        default=1,
        help="number of function slots of this library",
    )
    parser.add_argument(
        "--worker-pid",
        required=True,
        type=int,
        help="pid of main vine worker to send sigchild to let it know theres some result.",
    )
    args = parser.parse_args()

    # check if library cores and function slots are valid
    if args.function_slots > args.library_cores:
        stdout_timed_message("error: function slots cannot be more than library cores")
        exit(1)
    elif args.function_slots < 1:
        stdout_timed_message("error: function slots cannot be less than 1")
        exit(1)
    elif args.library_cores < 1:
        stdout_timed_message("error: library cores cannot be less than 1")
        exit(1)

    try:
        thread_limit = args.library_cores // args.function_slots
    except Exception as e:
        stdout_timed_message(f"error: {e}")
        exit(1)

    # check if the in_pipe_fd and out_pipe_fd are valid
    try:
        fcntl.fcntl(args.in_pipe_fd, fcntl.F_GETFD)
        fcntl.fcntl(args.out_pipe_fd, fcntl.F_GETFD)
    except IOError as e:
        stdout_timed_message(f"error: pipe fd closed\n{e}")
        exit(1)

    stdout_timed_message(f"library task starts running in process {os.getpid()}")
    stdout_timed_message(f"hostname             {socket.gethostname()}")
    stdout_timed_message(f"task id              {args.task_id}")
    stdout_timed_message(f"worker pid           {args.worker_pid}")
    stdout_timed_message(f"library pid          {os.getpid()}")
    stdout_timed_message(f"input fd             {args.in_pipe_fd}")
    stdout_timed_message(f"output fd            {args.out_pipe_fd}")
    stdout_timed_message(f"library cores        {args.library_cores}")
    stdout_timed_message(f"function slots       {args.function_slots}")
    stdout_timed_message(f"thread limit         {thread_limit}")

    # Open communication pipes to vine_worker.
    # The file descriptors are inherited from the vine_worker parent process
    # and should already be open for reads and writes.
    in_pipe_fd = args.in_pipe_fd
    out_pipe_fd = args.out_pipe_fd

    # mapping of child pid to function id of currently running functions
    pid_to_func_id = {}

    # read in information about this library
    with open('library_info.clpk', 'rb') as f:
        library_info = cloudpickle.load(f)

    # load and execute this library's context
    library_context_info = cloudpickle.loads(library_info['context_info'])
    context_vars = None
    if library_context_info:
        context_func = library_context_info[0]
        context_args = library_context_info[1]
        context_kwargs = library_context_info[2]
        context_vars = context_func(*context_args, **context_kwargs)

    # register functions in this library to the global namespace
    for func_name in library_info['function_list']:
        func_code = remote_execute(cloudpickle.loads(library_info['function_list'][func_name]))
        globals()[func_name] = func_code

    # update library's context to the load function
    if context_vars:
        (load_variable_from_library.__globals__).update(context_vars)

    # set execution mode of functions in this library
    global exec_method
    exec_method = library_info['exec_mode']

    # set infile load mode of functions in this library
    global function_infile_load_mode
    function_infile_load_mode = library_info['function_infile_load_mode']

    # send configuration of library, just its name for now
    config = {
        "name": library_info['library_name'],
        "taskid": args.task_id,
        "exec_mode": exec_method,
        "function_infile_load_mode": function_infile_load_mode,
    }
    send_configuration(config, out_pipe_fd, args.worker_pid)

    # register sigchld handler to turn a sigchld signal into an I/O event
    signal.signal(signal.SIGCHLD, sigchld_handler)

    # 5 seconds to wait for select, any value long enough would probably do
    timeout = 5

    last_check_time = time.time() - 5

    while True:
        # check if parent exits
        c_ppid = os.getppid()
        if c_ppid != ppid or c_ppid == 1:
            stdout_timed_message("library finished because parent exited")
            exit(0)

        # periodically log the number of concurrent functions
        current_check_time = time.time()
        if current_check_time - last_check_time >= 5:
            stdout_timed_message(f"{len(pid_to_func_id)} functions running concurrently")
            last_check_time = current_check_time

        # in case of "fork" exec method, wait for messages from worker or child to return
        # in case of "direct" exec method, wait for messages from worker
        try:
            rlist, wlist, xlist = select.select([in_pipe_fd, r], [], [], timeout)
        except Exception as e:
            stdout_timed_message(f"error unable to read from pipe {in_pipe_fd}\n{e}")

        for re in rlist:
            # worker has a function, run it
            if re == in_pipe_fd:
                if exec_method == 'direct':
                    _, func_id = start_function(in_pipe_fd, thread_limit)
                    send_result(
                        out_pipe_fd,
                        args.worker_pid,
                        func_id,
                        0,
                    )
                else:
                    pid, func_id = start_function(in_pipe_fd, thread_limit)
                    # pid == -1 indicates a failure during fork/setup of the function execution
                    if pid == -1:
                        send_result(
                            out_pipe_fd,
                            args.worker_pid,
                            func_id,
                            1,
                        )
                    else:
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
                        send_result(
                            out_pipe_fd,
                            args.worker_pid,
                            pid_to_func_id[c_pid],
                            c_exit_status,
                        )
                        del pid_to_func_id[c_pid]
                    # no exited child to reap, break
                    else:
                        break
    return 0


if __name__ == '__main__':
    main()


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
