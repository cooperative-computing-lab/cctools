
from . import cvine
import hashlib
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import FIRST_EXCEPTION
from concurrent.futures import ALL_COMPLETED
from concurrent.futures._base import PENDING
from concurrent.futures._base import CANCELLED
from concurrent.futures._base import FINISHED
from concurrent.futures import TimeoutError
from collections import namedtuple
from .task import (
    PythonTask,
    FunctionCall,
    PythonTaskNoResult,
    FunctionCallNoResult,
)
from .manager import (
    Factory,
    Manager,
)

import os
import time
import textwrap

RESULT_PENDING = 'result_pending'

try:
    import cloudpickle
    pythontask_available = True
except Exception:
    # Note that the intended exception here is ModuleNotFoundError.
    # However, that type does not exist in Python 2
    pythontask_available = False


# To be installed in the library for FutureFunctionCalls
def retrieve_output(arg):
    return arg


def wait(fs, timeout=None, return_when=ALL_COMPLETED):

    results = namedtuple('result', ['done', 'not_done'])
    results.done = set()
    results.not_done = set()

    # submit tasks if they have not been subitted
    for f in fs:
        if not f._is_submitted:
            f.module_manager.submit(f._task)

    time_init = time.time()
    if timeout is None:
        time_check = float('inf')
    else:
        time_check = timeout
    done = False
    while time.time() - time_init < time_check and not done:
        done = True
        for f in fs:

            # skip if future is complete
            if f in results.done:
                continue

            # check for completion
            result = f.result(timeout=5)

            # add to set of finished tasks and break when needed.
            if result != RESULT_PENDING:
                results.done.add(f)

                # if this is the first completed task, break.
                if return_when == FIRST_COMPLETED:
                    done = True
                    break

            if isinstance(result, Exception) and return_when == FIRST_EXCEPTION:
                done = True
                break

            # set done to false to finish loop.
            else:
                done = False

            # check form timeout
            if timeout is not None:
                if time.time() - time_init > timeout:
                    break

    # add incomplete futures to set
    for f in fs:
        if f not in results.done:
            results.not_done.add(f)

    return results


def as_completed(fs, timeout=None):

    results = set()

    # submit tasks if they have not been subitted
    for f in fs:
        if not f._is_submitted:
            f.module_manager.submit(f._task)

    time_init = time.time()
    if timeout is None:
        time_check = float('inf')
    else:
        time_check = timeout

    done = False
    while time.time() - time_init < time_check and not done:
        for f in fs:
            done = True
            # skip if future is complete
            if f in results:
                continue

            # check for completion
            result = f.result(timeout=5)

            # add to set of finished tasks
            if result != RESULT_PENDING:
                results.add(f)

            # set done to false to finish loop.
            else:
                done = False

            # check form timeout
            if timeout is not None:
                if time.time() - time_init > timeout:
                    break
    for f in fs:
        if f not in results:
            results.add(TimeoutError)

    return iter(results)


##
# \class FuturesExecutor
#
# TaskVine FuturesExecutor object
#
# This class acts as an interface for the creation of Futures
class FuturesExecutor(Executor):
    def __init__(self, port=9123, batch_type="local", manager=None, manager_host_port=None, manager_name=None, factory_binary=None, worker_binary=None, log_file=os.devnull, factory=True, opts={}):
        self.manager = Manager(port=port)
        self.port = self.manager.port
        if manager_name:
            self.manager.set_name(manager_name)
        self.manager_name = manager_name
        self.task_table = []
        if factory:
            self.factory = Factory(
                batch_type=batch_type,
                manager=manager,
                manager_host_port=manager_host_port,
                manager_name=manager_name,
                factory_binary=factory_binary,
                worker_binary=worker_binary,
                log_file=os.devnull,
            )
            self.set('min-workers', 5)
            for opt in opts:
                self.set(opt, opts[opt])
            self.factory.start()
        else:
            self.factory = None

    def submit(self, fn, *args, **kwargs):
        if isinstance(fn, FuturePythonTask):
            self.manager.submit(fn)
            fn._future._is_submitted = True
            return fn._future
        if isinstance(fn, FutureFunctionCall):
            self.manager.submit(fn)
            self.task_table.append(fn)
            fn._future._is_submitted = True
            return fn._future
        future_task = self.future_task(fn, *args, **kwargs)
        self.task_table.append(future_task)
        future_task._future._is_submitted = True
        self.submit(future_task)
        return future_task._future

    def future_task(self, fn, *args, **kwargs):
        return FuturePythonTask(self.manager, False, fn, *args, **kwargs)

    def create_library_from_functions(self, name, *function_list, poncho_env=None, init_command=None, add_env=True, import_modules=None):
        return self.manager.create_library_from_functions(name, *function_list, retrieve_output, poncho_env=poncho_env, init_command=init_command, add_env=add_env, import_modules=import_modules)

    def install_library(self, libtask):
        self.manager.install_library(libtask)

    def future_funcall(self, library_name, fn, *args, **kwargs):
        return FutureFunctionCall(self.manager, False, library_name, fn, *args, **kwargs)

    def set(self, name, value):
        if self.factory:
            return self.factory.__setattr__(name, value)

    def get(self, name):
        if self.factory:
            return self.factory.__getattr__(name)

    def __del__(self):
        for task in self.task_table:
            if hasattr(task, '_retriever') and task._retriever:
                task._retriever.__del__()
            task.__del__()
        self.manager.__del__()


##
# \class Vinefuture
#
# TaskVine VineFuture Object
#
# An instance of this class can re resolved to a value that will be computed asynchronously
class VineFuture(Future):
    def __init__(self, task):
        super().__init__()
        self._state = PENDING
        self._task = task
        self._callback_fns = []
        self._result = None
        self._is_submitted = False

    def cancel(self):
        self._task._module_manager.cancel_by_task_id(self._task.id)
        self._state = CANCELLED

    def cancelled(self):
        state = self._task._module_manager.task_state(self._task.id)
        if state == cvine.VINE_TASK_CANCELED:
            return True
        else:
            return False

    def running(self):
        state = self._task._module_manager.task_state(self._task.id)
        if state == cvine.VINE_TASK_RUNNING:
            return True
        else:
            return False

    def done(self):
        state = self._task._module_manager.task_state(self._task.id)
        if state == cvine.VINE_TASK_DONE:
            return True
        else:
            return False

    def result(self, timeout="wait_forever"):
        if timeout is None:
            timeout = "wait_forever"
        result = self._task.output(timeout=timeout)
        if result == RESULT_PENDING:
            return RESULT_PENDING
        else:
            self._result = result
            self._state = FINISHED
            return result

    def add_done_callback(self, fn):
        self._callback_fns.append(fn)


##
# \class FutureFunctionCall
#
# TaskVine FutureFunctionCall object
#
# This class is a sublcass of FunctionCall that is specialized for future execution

class FutureFunctionCall(FunctionCall):
    def __init__(self, manager, is_retriever, library_name, fn, *args, **kwargs):
        super().__init__(library_name, fn, *args, **kwargs)
        self.enable_temp_output()
        self.manager = manager
        self.library_name = library_name
        self._envs = []

        self._future = VineFuture(self)
        self._is_retriever = is_retriever
        self._has_retrieved = False
        self._retriever = None
        self._ran_functions = False

    # Set a retriever for this task, it has to disable temp_output b/c
    # it aims to bring the remote output back to the manager
    def set_retriever(self):
        self._retriever = FutureFunctionCall(self.manager, True, self.library_name, 'retrieve_output', self._future)
        self._retriever.disable_temp_output()
        self.manager.submit(self._retriever)

    # Given that every designated task stores its outcome in a temp file,
    # this function is invoked through `VineFuture.result()` to trigger its retriever
    # to bring that output back to the manager.
    def output(self, timeout="wait_forever"):

        # when output() is called, set a retriever task to bring back the on-worker output
        if not self._is_retriever and not self._retriever:
            self.set_retriever()

        # for retrievee task: wait for retriever to get result
        if not self._is_retriever:
            if self._saved_output:
                return self._saved_output
            result = self._retriever.output(timeout=timeout)
            if result is RESULT_PENDING:
                return RESULT_PENDING
            self._saved_output = result
            if not self._ran_functions:
                for fn in self._future._callback_fns:
                    fn(self._future)
                self._ran_functions = True
            return self._saved_output

        # for retriever task: fetch the result of its retrievee on completion
        if self._is_retriever:
            if not self._has_retrieved:
                result = self._manager.wait_for_task_id(self.id, timeout=timeout)
                if result:
                    self._has_retrieved = True
                else:
                    return RESULT_PENDING
            if not self._saved_output and self._has_retrieved:
                if self.successful():
                    try:
                        output = cloudpickle.loads(self._output_file.contents())
                        if output['Success']:
                            self._saved_output = output['Result']
                        else:
                            self._saved_output = output['Reason']

                    except Exception as e:
                        self._saved_output = e
                else:
                    self._saved_output = FunctionCallNoResult()
            return self._saved_output

    # gather results from preceding tasks to use as inputs for this specific task
    def submit_finalize(self):
        new_fn_args = []
        arg_id = 0
        for arg in self._event['fn_args']:
            arg_id += 1
            if isinstance(arg, VineFuture):
                # for the task where the input comes from the output of a prior task, add that output file as its input
                # the purpose of using hash digest is to avoid using the name for multiple input files
                input_filename = str(arg_id) + str(arg._task.id) + str(self.id)
                input_filename = hashlib.md5(input_filename.encode()).hexdigest()
                self.add_input(arg._task._output_file, input_filename)
                # create a special format {'VineFutureFile': 'outfile-x'} to indicate loading input from a tempfile
                new_fn_args.append({"VineFutureFile": input_filename})
            else:
                new_fn_args.append(arg)
        self._event['fn_args'] = tuple(new_fn_args)

        super().submit_finalize()

    def __del__(self):
        super().__del__()


##
# \class FuturePythonTask
#
# TaskVine FuturePythonTask object
#
# This class is a sublcass of PythonTask that is specialized for futures

class FuturePythonTask(PythonTask):

    ##
    # Creates a new Future Task
    #
    # @param self
    # @param func
    # @param args
    # @param kwargs
    def __init__(self, manager, rf, func, *args, **kwargs):
        super(FuturePythonTask, self).__init__(func, *args, **kwargs)
        self.enable_temp_output()
        self._module_manager = manager
        self._future = VineFuture(self)
        self._envs = []
        self._has_retrieved = False
        self._ran_functions = False
        self._is_retriever = rf
        self._retriever = None

    def output(self, timeout="wait_forever"):
        def retrieve_output(arg):
            return arg
        if not self._is_retriever and not self._retriever:
            self._retriever = FuturePythonTask(self._module_manager, True, retrieve_output, self._future)
            self._retriever.set_cores(1)
            for env in self._envs:
                self._retriever.add_environment(env)
            self._retriever.disable_temp_output()
            self._module_manager.submit(self._retriever)

        if not self._is_retriever:
            if not self._output_loaded:
                result = self._retriever._future.result(timeout=timeout)
                if result == RESULT_PENDING:
                    return RESULT_PENDING
                self._output = result
                self._output_loaded = True
            if not self._ran_functions:
                for fn in self._future._callback_fns:
                    fn(self._future)
                self._ran_functions = True
            return self._output

        else:
            if not self._has_retrieved:
                result = self._module_manager.wait_for_task_id(self.id, timeout=timeout)
                if result:
                    self._has_retrieved = True
                else:
                    return RESULT_PENDING
            if not self._output_loaded and self._has_retrieved:
                if self.successful():
                    try:
                        with open(self._output_file.source(), "rb") as f:
                            if self._serialize_output:
                                self._output = cloudpickle.load(f)
                            else:
                                self._output = f.read()
                    except Exception as e:
                        self._output = e
                else:
                    self._output = PythonTaskNoResult()
                self._output_loaded = True
            return self._output

    def submit_finalize(self):
        func, args, kwargs = self._fn_def

        new_fn_args = []
        arg_id = 0
        for arg in args:
            arg_id += 1
            if isinstance(arg, VineFuture):
                input_filename = str(arg_id) + str(arg._task.id) + str(self.id)
                input_filename = hashlib.md5(input_filename.encode()).hexdigest()
                self.add_input(arg._task._output_file, input_filename)
                new_fn_args.append({"VineFutureFile": input_filename})
            else:
                new_fn_args.append(arg)

        args = new_fn_args
        self._fn_def = (func, args, kwargs)

        super().submit_finalize()

    def add_environment(self, f):
        self._envs.append(f)
        return cvine.vine_task_add_environment(self._task, f._file)

    def _fn_wrapper(self, manager, serialize):
        base = f"py_futures_wrapper_{int(bool(serialize))}"
        if base not in manager._function_buffers:
            name = os.path.join(manager.staging_directory, base)
            with open(name, "w") as f:
                f.write(
                    textwrap.dedent(
                        f"""
                        import sys
                        import cloudpickle

                        def vineLoadArg(arg):
                            with open (arg["VineFutureFile"] , 'rb') as f:
                                return cloudpickle.load(f)

                        (fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
                        with open (fn , 'rb') as f:
                            exec_function = cloudpickle.load(f)
                        with open(args, 'rb') as f:
                            args, kwargs = cloudpickle.load(f)

                        args = [vineLoadArg(arg) if isinstance(arg, dict) and "VineFutureFile" in arg else arg for arg in args]
                        error = None
                        try:
                            exec_out = exec_function(*args, **kwargs)
                        except Exception as e:
                            exec_out = e
                            error = e
                        finally:
                            with open(out, 'wb') as f:
                                if {serialize}:
                                    cloudpickle.dump(exec_out, f)
                                else:
                                    f.write(exec_out)
                            if error:
                                raise error
                        """
                    )
                )
            manager._function_buffers[base] = manager.declare_file(name, cache=True)
        return manager._function_buffers[base]

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
