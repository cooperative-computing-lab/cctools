
from . import cvine
from concurrent.futures import Executor
from concurrent.futures import Future
from .task import (
    PythonTask,
    FunctionCall,
    PythonTaskNoResult,
)
from .manager import (
    Factory,
    Manager,
)

import os
import textwrap
import tempfile

try:
    import cloudpickle
    pythontask_available = True
except Exception:
    # Note that the intended exception here is ModuleNotFoundError.
    # However, that type does not exist in Python 2
    pythontask_available = False

##
# \class Executor
#
# TaskVine Executor object
#
# This class acts as an interface for the creation of Futures

class Executor(Executor):
    def __init__(self, port=9123, batch_type="local", manager=None, manager_host_port=None, manager_name=None, factory_binary=None, worker_binary=None, log_file=os.devnull, factory=True, opts={}):
        self.manager = Manager(port=port)
        self.port = self.manager.port
        if manager_name:
            self.manager.set_name(manager_name)
        self.manager_name = manager_name
        self.task_table = []
        if factory:
            self.factory = Factory(batch_type=batch_type, manager=manager, manager_host_port=manager_host_port, manager_name=manager_name, 
                    factory_binary=factory_binary, worker_binary=worker_binary, log_file=os.devnull)
            self.set('min-workers', 5)
            for opt in opts:
                self.set(opt, opts[opt])
            self.factory.start()
        else: 
            self.factory = None

    def submit(self, task):
        if isinstance(task, FuturePythonTask):
            self.manager.submit(task)
            return task._future
        if isinstance(task, FutureFunctionCall):
            self.manager.submit(task)
            self.manager.submit(task.retriver)
            self.task_table.append(task)
            self.task_table.append(task.retriver)
            return
        raise TypeError("task must be a FuturePythonTask or FutureFunctionCall object")

    def future_pythontask(self, fn, *args, **kwargs):
        return FuturePythonTask(self.manager, False, fn, *args, **kwargs)
    
    def create_library_from_functions(self, name, *function_list, poncho_env=None, init_command=None, add_env=True, import_modules=None):
        return self.manager.create_library_from_functions(name, *function_list, poncho_env=poncho_env, init_command=init_command, add_env=add_env, import_modules=import_modules)
    
    def install_library(self, libtask):
        self.manager.install_library(libtask)

    def future_funcall(self, library_name, fn, *args, **kwargs):
        future_funcall = FutureFunctionCall(self.manager, False, library_name, fn, *args, **kwargs)
        future_funcall_retriver = FutureFunctionCall(self.manager, True, library_name, fn, *args, **kwargs)
        future_funcall.retriver = future_funcall_retriver
        future_funcall_retriver.retrivee = future_funcall
        return future_funcall

    def set(self, name, value):
        if self.factory:
            return self.factory.__setattr__(name, value)

    def get(self, name):
        if self.factory:
            return self.factory.__getattr__(name)
        
    def __del__(self):
        for task in self.task_table:
            task.__del__()
        self.manager.__del__()


class FutureFunctionCall(FunctionCall):
    def __init__(self, manager, is_retrival_task, library_name, fn, *args, **kwargs):
        super().__init__(library_name, fn, *args, **kwargs)
        self._manager = manager
        self.library_name = library_name
        self._envs = []
        self._ran_functions = False
        self._is_retriver = is_retrival_task
        self._retrival_task = None
        self._mother_task = None
        self.retriver = None
        self.retrivee = None
        self._cache_output = True
    
    def __del__(self):
        super().__del__()
    
    def submit_finalize(self, manager):
        return super().submit_finalize(manager)

    @property
    def output(self, timeout="wait_forever"):
        # for retrival task: load output of the primary task
        if self._is_retriver:
            self._manager.wait_for_task_id(self.retrivee.id, timeout=timeout)
            if self.retrivee.successful():
                output = cloudpickle.loads(self.retrivee._output_buffer.contents())
                if output['Success']:
                    if self._cache_enabled:
                        self._cached_output = output['Result']
                    return output['Result']
                else:
                    return output['Reason']
        
        # for regular function call: invoke retrival function call
        if not self._is_retriver:
            if self._cached_output:
                return self._cached_output
            self._manager.wait_for_task_id(self.retriver.id, timeout=timeout)
            if self.retriver.successful():
                self._cached_output = self.retriver.output
                return self._cached_output
##
# \class Vinefuture
#
# TaskVine VineFuture Object
#
# An instance of this class can re resolved to a value that will be computed asynchronously

class VineFuture(Future):
    def __init__(self, task):
        self._task = task
        self._callback_fns = []
    def cancel(self):
        self._task._module_manager.cancel_by_task_id(self._task.id)
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
        return self._task.output(timeout=timeout)
    def add_done_callback(self, fn):
        self.callback_fns.append(fn)

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
        self._future_resolved = False
        self._ran_functions = False
        self._retrieval_future = rf
        self._retrieval_task = None

    def output(self, timeout="wait_forever"):
        def retrieve_output(arg):
            return arg
        if not self._retrieval_future and not self._retrieval_task:
            self._retrieval_task = FuturePythonTask(self._module_manager, True, retrieve_output, self._future)
            self._retrieval_task.set_cores(1)
            for env in self._envs:
                self._retrieval_task.add_environment(env)
            self._retrieval_task.disable_temp_output()
            self._module_manager.submit(self._retrieval_task)

        if not self._retrieval_future:
            if not self._output_loaded:
                self._output = self._retrieval_task._future.result(timeout=timeout)
                self._output_loaded = True
            if not self._ran_functions:
                for fn in self._future._callback_fns:
                    fn(self._output)
                self._ran_functions = True
            return self._output
    
        else:
            if not self._future_resolved:
                result = self._module_manager.wait_for_task_id(self.id, timeout=timeout)
                if result:
                    self._future_resolved = True
            if not self._output_loaded and self._future_resolved:
                if self.successful():
                    try:
                        with open(os.path.join(self._tmpdir, self._out_name_file), "rb") as f:
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

    def add_future_dep(self, arg):
        self.add_input(arg._task._output_file, str('outfile-' + str(arg._task.id)))

    def submit_finalize(self, manager):
        self._manager = manager
        func, args, kwargs = self._fn_def
        for arg in args:
            if isinstance(arg, VineFuture):
                self.add_future_dep(arg)
        args = [{"VineFutureFile": str('outfile-' + str(arg._task.id))} if isinstance(arg, VineFuture) else arg for arg in args]
        self._fn_def = (func, args, kwargs)
        self._tmpdir = tempfile.mkdtemp(dir=manager.staging_directory)
        self._serialize_python_function(*self._fn_def)
        self._fn_def = None  # avoid possible memory leak
        self._create_wrapper()
        self._add_IO_files(manager)

    def add_environment(self, f):
        self._envs.append(f)
        return cvine.vine_task_add_environment(self._task, f._file)

    def _create_wrapper(self):
        with open(os.path.join(self._tmpdir, self._wrapper), "w") as f:
            f.write(
                textwrap.dedent(
                f"""

                try:
                    import sys
                    import cloudpickle
                except ImportError as e:
                    print("Could not execute PythonTask function because a module was not available at the worker.")
                    raise

                def vineLoadArg(arg):
                    with open (arg["VineFutureFile"] , 'rb') as f:
                        return cloudpickle.load(f)

                (fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
                with open (fn , 'rb') as f:
                    exec_function = cloudpickle.load(f)
                with open(args, 'rb') as f:
                    args, kwargs = cloudpickle.load(f)
                

                args = [vineLoadArg(arg) if isinstance(arg, dict) and "VineFutureFile" in arg else arg for arg in args]
                status = 0 
                try:
                    exec_out = exec_function(*args, **kwargs)
                except Exception as e:
                    exec_out = e
                    status = 1

                with open(out, 'wb') as f:
                    if {self._serialize_output}:
                        cloudpickle.dump(exec_out, f)
                    else:
                        f.write(exec_out)

                sys.exit(status)
                """
                )
            )


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
