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
    import ctypes
    from ctypes.util import find_library
    from contextlib import ContextDecorator

    class CLibController:
        def __init__(self, category, lib_paths, set_func_names, get_func_names):
            self.category = category
            self.lib = None
            for path in lib_paths:
                try:
                    self.lib = ctypes.CDLL(path)
                    break
                except OSError:
                    continue
            if not self.lib:
                raise ValueError(f"Failed to load library for category '{self.category}' from paths: {lib_paths}")

            self.set_num_threads_func = self._find_valid_func(set_func_names)
            self.get_num_threads_func = self._find_valid_func(get_func_names)

            if not self.set_num_threads_func or not self.get_num_threads_func:
                raise ValueError(f"Failed to find valid set/get thread functions for category '{self.category}'.")

        def _find_valid_func(self, func_names):
            for name in func_names:
                func = getattr(self.lib, name, None)
                if func:
                    return func
            return None

        def set_num_threads(self, num_threads):
            if self.set_num_threads_func:
                self.set_num_threads_func(num_threads)

        def get_num_threads(self):
            if self.get_num_threads_func:
                return self.get_num_threads_func()
            return None

    class ThreadpoolLimits(ContextDecorator):
        def __init__(self, limits=1):
            self.limits = limits
            self.lib_controllers = []
            self.original_limits = {}
            self._init_controllers()

        def _get_libc(self):
            libc_path = find_library("c")
            if libc_path:
                return ctypes.CDLL(libc_path)
            return None
        
        def __enter__(self):
            # Set thread limits
            for controller in self.lib_controllers:
                if controller.lib is not None:
                    original_num = controller.get_num_threads()
                    if original_num is not None:
                        self.original_limits[controller] = original_num
                        controller.set_num_threads(self.limits)
            return self

        def __exit__(self, *exc):
            # Restore original thread limits
            for controller, original_num in self.original_limits.items():
                if controller.lib is not None:
                    controller.set_num_threads(original_num)
            return False

        def _init_controllers(self):
            lib_definitions = [
                {
                    "category": "openblas",
                    "prefixes": ["openblas", "libopenblas", "libblas"],
                    "set_funcs": ["openblas_set_num_threads", "openblas_set_num_threads64_"],
                    "get_funcs": ["openblas_get_num_threads", "openblas_get_num_threads64_"]
                },
                {
                    "category": "blis",
                    "prefixes": ["blis", "libblis", "libblas"],
                    "set_funcs": ["bli_thread_set_num_threads"],
                    "get_funcs": ["bli_thread_get_num_threads"]
                },
                {
                    "category": "flexiblas",
                    "prefixes": ["flexiblas", "libflexiblas"],
                    "set_funcs": ["flexiblas_set_num_threads"],
                    "get_funcs": ["flexiblas_get_num_threads"]
                },
                {
                    "category": "mkl",
                    "prefixes": ["mkl_rt", "libmkl_rt", "mkl_core", "libmkl_rt", "libblas"],
                    "set_funcs": ["MKL_Set_Num_Threads", "mkl_set_num_threads"],
                    "get_funcs": ["MKL_Get_Max_Threads", "mkl_get_max_threads"]
                },
                {
                    "category": "openmp",
                    "prefixes": ["gomp", "libgomp", "iomp5", "libiomp5", "libomp", "vcomp", "vcomp140"],
                    "set_funcs": ["omp_set_num_threads"],
                    "get_funcs": ["omp_get_max_threads", "omp_get_num_procs"]
                }
            ]

            for lib_def in lib_definitions:
                matched_paths = []
                for prefix in lib_def["prefixes"]:
                    lib_path = find_library(prefix)
                    if lib_path:
                        matched_paths.append(lib_path)
                        break

                if matched_paths:
                    try:
                        controller = CLibController(
                            category=lib_def["category"],
                            lib_paths=matched_paths,
                            set_func_names=lib_def["set_funcs"],
                            get_func_names=lib_def["get_funcs"]
                        )
                        self.lib_controllers.append(controller)
                    except ValueError as e:
                        print(f"Error initializing controller for category '{lib_def['category']}': {e}")

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
    def start_function(in_pipe_fd, library_cores, function_slots):
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

        if function_name:
            # exec method for now is fork only, direct will be supported later
            exec_method = 'fork'
            with ThreadpoolLimits(limits=library_cores // function_slots):
                # also set related environment variables, not necessary for some situations
                cores_env = str(library_cores // function_slots)
                os.environ['OMP_NUM_THREADS'] = cores_env
                os.environ['CORES'] = cores_env
                os.environ['CORES'] = cores_env
                os.environ['OPENBLAS_NUM_THREADS'] = cores_env
                os.environ['VECLIB_NUM_THREADS'] = cores_env
                os.environ['MKL_NUM_THREADS'] = cores_env
                os.environ['NUMEXPR_NUM_THREADS'] = cores_env
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
        else:
            # malformed message from worker so we exit
            print('malformed message from worker. Exiting..', file=sys.stderr)
            exit(1)

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
        parser.add_argument('--task-id', required=True, type=int, help='task id of this library task')
        parser.add_argument('--library-cores', required=True, type=int, help='number of cores to use for library functions')
        parser.add_argument('--function-slots', required=True, type=int, help='number of function slots to use for library functions')
        parser.add_argument('--worker-pid', required=True, type=int, help='pid of main vine worker to send sigchild to let it know theres some result.')
        args = parser.parse_args()

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
                    library_cores = args.library_cores
                    function_slots = args.function_slots
                    pid, func_id = start_function(in_pipe_fd, library_cores, function_slots)
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
