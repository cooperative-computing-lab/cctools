
from . import cvine
import hashlib
from collections import deque
from concurrent.futures import Executor
from concurrent.futures import Future
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import FIRST_EXCEPTION
from concurrent.futures import ALL_COMPLETED
from concurrent.futures._base import PENDING
from concurrent.futures._base import CANCELLED
from concurrent.futures._base import FINISHED
from concurrent.futures import TimeoutError
from collections import namedtuple, deque
from .task import (
    PythonTask,
    FunctionCall,
    FunctionCallNoResult,
)
from .manager import (
    Factory,
    Manager,
)

import os
import time
import textwrap
import inspect

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

    start = time.perf_counter()
    time_check = float('inf') if timeout is None else timeout
    result_timeout = min(timeout, 5) if timeout is not None else 5

    done = False
    while time.perf_counter() - start < time_check and not done:
        for f in fs:
            # skip if future is complete
            if f in results.done:
                continue

            try:
                # check for completion
                f.result(timeout=result_timeout)
            except TimeoutError:
                # TimeoutError's are expected since we are polling the
                # future's status. If a timeout happens, we do nothing.
                pass
            except Exception:
                # Future.result() raises the task's exception but that is
                # not relevant here---just if the task has finished.
                results.done.add(f)
                if return_when in (FIRST_EXCEPTION, FIRST_COMPLETED):
                    done = True
                    break
            else:
                results.done.add(f)
                if return_when == FIRST_COMPLETED:
                    done = True
                    break

            if len(results.done) == len(fs):
                done = True
                break

            # check form timeout
            if time.perf_counter() - start > time_check:
                break

    # add incomplete futures to set
    for f in fs:
        if f not in results.done:
            results.not_done.add(f)

    return results


def as_completed(fs, timeout=None):
    fs = deque(fs)

    # submit tasks if they have not been submitted
    for f in fs:
        if not f._is_submitted:
            f.module_manager.submit(f._task)

    start = time.perf_counter()
    result_timeout = min(timeout, 5) if timeout is not None else 5

    def _iterator():
        # iterate of queue of futures, yeilding completed futures and
        # requeuing non-completed futures until all futures are yielded or
        # the timeout is reached.
        while fs:
            f = fs.popleft()

            try:
                result = f.result(timeout=result_timeout)
            except TimeoutError:
                # TimeoutError's are expected since we are polling the
                # future's status. If a timeout happens, add the future to
                # the back of the queue.
                fs.append(f)
            except Exception:
                # Future.result() raises the task's exception but that is
                # not relevant here---just if the task has finished.
                yield f
            else:
                assert result != RESULT_PENDING
                yield f

            if (
                fs and timeout is not None
                and time.perf_counter() - start > timeout
            ):
                raise TimeoutError()

    return _iterator()

def run_iterable(fn, iterable, dimensions=1):
    if not ((hasattr(iterable, '__iter__') or hasattr(iterable, '__getitem__')) and not isinstance(iterable, str)):
        return fn(element)
    if dimensions < 1:
        return None
    result = []
    if dimensions == 1:
        for element in iterable:
            if (hasattr(element, '__iter__') or hasattr(element, '__getitem__')) and not isinstance(element, str):
                result.append(fn(*element))
            else:
                result.append(fn(element))
    else:
        for inner_iterable in iterable:
            result.append(run_iterable(fn, inner_iterable, dimensions-1))
    return result

def reduction_tree(fn, *args):
    minimum_parameters = len(inspect.signature(fn).parameters)
    curr_size = len(args)
    entries = deque([f.result() if isinstance(f, VineFuture) else f for f in args])
    return_val = entries
    while curr_size >= minimum_parameters:
        parameters = []
        for _ in range(minimum_parameters):
            parameters.append(entries.popleft())
        new_result = fn(*parameters)
        if (hasattr(new_result, '__getitem__') or hasattr(new_result, '__iter__')) and not isinstance(new_result, str):
            for result in new_result:
                entries.appendleft(result)
        else:
            entries.appendleft(new_result)
        curr_size = len(entries)
    return_val = entries if len(entries) > 1 else entries[0]
    return entries[0]

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

    def map(self, fn, iterable, library_name=None, method=None, chunk_size=1):
        def wait_for_map_resolution(*futures_batch):
            result = []
            for computed_result in futures_batch:
                result.extend(computed_result) 
            return result
        if (hasattr(iterable, '__iter__') or hasattr(iterable, '__getitem__')) and not isinstance(iterable, str): 
            tasks = []
            for i in range(0, len(iterable), chunk_size):
                if method == "FutureFunctionCall":
                    future_batch_task = self.submit(self.future_funcall(library_name, run_iterable, fn, iterable[i:i+chunk_size]))
                else: # Method is FuturePythonTask
                    future_batch_task = self.submit(run_iterable, fn, iterable[i:i+chunk_size])
                tasks.append(future_batch_task)
            future = self.submit(wait_for_map_resolution, *tasks)
        else:
            if method == "FutureFunctionCall":
                future = self.submit(self.future_funcall(library_name, run_iterable, fn, iterable))
            else: # Method is FuturePythonTask
                future = self.submit(run_iterable, fn, iterable)
        return future

    # Reduce performs a reduction tree on the iterable and currently returns a single value
    #
    # parameters:
    # - Function
    # - Iterable of parameters that function will take
    # - a library_name for a library function call
    # - a method
    # - a chunk_size, which is the number of iterations to complete in one task. if c is chunk_size, a single task will reduce c(n-1) + 1 nodes to 1 node

    def reduce(self, fn, iterable, library_name=None, method=None, chunk_size=1):
        # This line is just the identity - since when a future is pickled, it actually becomes some file, which means it is evaluated immediately/sent to queue
        if (hasattr(iterable, '__iter__') or hasattr(iterable, '__getitem__')) and not isinstance(iterable, str): 
            sub_futures = [iterable]
            num_parameters = len(inspect.signature(fn).parameters)
            reduction_size = chunk_size*(num_parameters-1)
            while len(sub_futures[-1]) > 1 or len(sub_futures) == 1:
                layer = []
                for i in range(0, len(sub_futures[-1]), reduction_size):
                    if method == "FutureFunctionCall":
                        future_batch_task = self.submit(self.future_funcall(library_name, reduction_tree, fn, *[self.submit(fetch_future_result, f) if isinstance(f, VineFuture) else f for f in sub_futures[-1][i:i+reduction_size]]))
                    else: # Method is FuturePythonTask
                        future_batch_task = self.submit(reduction_tree, fn, *[f if isinstance(f, VineFuture) else f for f in sub_futures[-1][i:i+reduction_size]])
                    layer.append(future_batch_task)
                sub_futures.append(layer)
            future = sub_futures[-1][0]
        # if this is a single value
        else:
            if method == "FutureFunctionCall":
                future = self.submit(self.future_funcall(library_name, reduction_tree, fn, iterable))
            else:
                future = self.submit(reduction_tree, fn, iterable)
        return future

    def allpairs(self, fn, iterable_a, iterable_b, library_name=None, method=None, chunk_size=1):
        def wait_for_allpairs_resolution(row_size, *futures_batch):
            result = []
            for computed_result in futures_batch:
                result.extend(computed_result) 
            processed_result = []
            for i in range(len(result)//row_size):
                row = result[i*row_size:i*row_size+row_size]
                processed_result.append(row)
            return processed_result
        iterable = [(a, b) for b in iterable_b for a in iterable_a]
        if (hasattr(iterable, '__iter__') or hasattr(iterable, '__getitem__')) and not isinstance(iterable, str): 
            tasks = []
            for i in range(0, len(iterable), chunk_size):
                if method == "FutureFunctionCall":
                    future_batch_task = self.submit(self.future_funcall(library_name, run_iterable, fn, iterable[i:i+chunk_size]))
                else: # Method is FuturePythonTask
                    future_batch_task = self.submit(run_iterable, fn, iterable[i:i+chunk_size])
                tasks.append(future_batch_task)
            future = self.submit(wait_for_allpairs_resolution, len(iterable_b), *tasks)
        else:
            if method == "FutureFunctionCall":
                future = self.submit(self.future_funcall(library_name, run_iterable, fn, iterable))
            else: # Method is FuturePythonTask
                future = self.submit(run_iterable, fn, iterable)
        return future
       
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
        return FuturePythonTask(self.manager, fn, *args, **kwargs)

    def create_library_from_functions(self, name, *function_list, poncho_env=None, init_command=None, add_env=True, hoisting_modules=None):
        return self.manager.create_library_from_functions(name, *function_list, retrieve_output, poncho_env=poncho_env, init_command=init_command, add_env=add_env, hoisting_modules=hoisting_modules)

    def install_library(self, libtask):
        self.manager.install_library(libtask)

    def future_funcall(self, library_name, fn, *args, **kwargs):
        return FutureFunctionCall(self.manager, library_name, fn, *args, **kwargs)

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
        self._exception = None
        self._is_submitted = False
        self._ran_functions = False

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
            raise TimeoutError()

        if isinstance(result, Exception):
            self._exception = result
        else:
            self._result = result

        self._state = FINISHED
        if self._callback_fns and not self._ran_functions:
            self._ran_functions = True
            for fn in self._callback_fns:
                fn(self)

        if isinstance(result, Exception):
            raise result
        return result

    def exception(self, timeout="wait_forever"):
        try:
            self.result()
        except Exception as e:
            return e
        else:
            return None

    def add_done_callback(self, fn):
        self._callback_fns.append(fn)


##
# \class FutureFunctionCall
#
# TaskVine FutureFunctionCall object
#
# This class is a sublcass of FunctionCall that is specialized for future execution

class FutureFunctionCall(FunctionCall):
    def __init__(self, manager, library_name, fn, *args, **kwargs):
        super().__init__(library_name, fn, *args, **kwargs)
        self.enable_temp_output()
        self.manager = manager
        self.library_name = library_name
        self._envs = []
        self._future = VineFuture(self)
        self._has_retrieved = False

    # Given that every designated task stores its outcome in a temp file,
    # we must first fetch the file before retruning the result.
    # to bring that output back to the manager.
    def output(self, timeout="wait_forever"):
        
        if not self._has_retrieved:
            result = self.manager.wait_for_task_id(self.id, timeout=timeout)
            if result:
                self._has_retrieved = True
            else:
                return RESULT_PENDING

        if not self._saved_output and self._has_retrieved:
            if self.successful():
                try:
                    self.manager.fetch_file(self._output_file)
                    output = cloudpickle.loads(self._output_file.contents())
                    if output['Success']:
                        self._saved_output = output['Result']
                    else:
                        self._saved_output = FunctionCallNoResult(output['Reason'])

                except Exception as e:
                    self._saved_output = e
            else:
                self._saved_output = FunctionCallNoResult()
            self._output_loaded = True
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
    def __init__(self, manager, func, *args, **kwargs):
        super(FuturePythonTask, self).__init__(func, *args, **kwargs)
        self.enable_temp_output()
        self._module_manager = manager
        self._future = VineFuture(self)
        self._envs = []
        self._has_retrieved = False

    def output(self, timeout="wait_forever"):

        # wait for task to complete if it has not been completed
        if not self._has_retrieved:
            result = self._module_manager.wait_for_task_id(self.id, timeout=timeout)
            if result:
                self._has_retrieved = True
            else:
                return RESULT_PENDING

        # fetch output file and load output
        if not self._output_loaded and self._has_retrieved:
            try:
                self._module_manager.fetch_file(self._output_file)
                # _output can be either the return value of a successful
                # task or the exception object of a failed task.
                self._output = cloudpickle.loads(self._output_file.contents())
            except Exception as e:
                print(self._output_file.contents())
                # handle output file fetch/deserialization failures
                self._output = e
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
