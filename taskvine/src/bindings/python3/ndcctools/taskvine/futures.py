
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
            # submit the task
            self.manager.submit(task)
            # append it to the task table
            self.task_table.append(task)
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
        # create and submit a retriver once the funcall is created
        future_funcall.set_retriver()
        # append it to the task table
        self.task_table.append(future_funcall.retriver)
        return future_funcall

    def set(self, name, value):
        if self.factory:
            return self.factory.__setattr__(name, value)

    def get(self, name):
        if self.factory:
            return self.factory.__getattr__(name)
        
    def __del__(self):
        for task in self.task_table:
            if hasattr(task, 'retriver') and task.retriver:
                task.retriver.__del__()
            task.__del__()
        self.manager.__del__()


##
# \class FutureFunctionCall
#
# TaskVine FutureFunctionCall object
#
# This class is a sublcass of FunctionCall that is specialized for future execution

class FutureFunctionCall(FunctionCall):
    def __init__(self, manager, is_retriver, library_name, fn, *args, **kwargs):
        super().__init__(library_name, fn, *args, **kwargs)
        self.manager = manager
        self.library_name = library_name
        self._is_retriver = is_retriver
        self._has_retrieved = False
        self.retriver = None
        self.retrivee = None
        self._cache_enabled = True

    # set a retriver for this task, there are two tasks running simutaneously,
    # once called the `output` on retriver, it will wait for the completion of 
    # its retrivee and bring back its output
    def set_retriver(self):
        self.retriver = FutureFunctionCall(self.manager, True, self.library_name, 'None', None)
        self.retriver.retrivee = self
        self.manager.submit(self.retriver)

    # just as that of a regular FunctionCall task, the user can call task.output 
    # to get the result of it
    @property
    def output(self, timeout="wait_forever"):
        # for retriver: fetch the result of retrivee on completion
        if self._is_retriver:
            if self._has_retrieved:
                return self.retrivee._cached_output
            # in the environment of multithreading, the dependent task might
            # have not been submitted, this task should wait for its submission
            while True:
                if self.retrivee.id:
                    break
            self.manager.wait_for_task_id(self.retrivee.id, timeout=timeout)
            if self.retrivee.successful():
                output = cloudpickle.loads(self.retrivee._output_buffer.contents())
                self._has_retrieved = True
                self.retrivee._has_retrieved = True
                if output['Success']:
                    if self.retrivee._cache_enabled:
                        self.retrivee._cached_output = output['Result']
                        self._cached_output = output['Result']
                    return output['Result']
                else:
                    return output['Reason']
            else:
                print(f"Warning: task {self.retrivee.id} was failed")
        
        # for retrivee: wait for retriver to get result
        if not self._is_retriver:
            if self._cached_output:
                return self._cached_output
            self.manager.wait_for_task_id(self.retriver.id, timeout=timeout)
            if self.retriver.successful():
                self._cached_output = self.retriver.output
                return self._cached_output
            else:
                print(f"Warning: task {self.retriver.id} was failed")
    
    # this will return whether the retriver or the already cached output of a normal task on the user's site.
    # the user should call task.future to make the future output of another task as the input of this task.
    @property
    def future(self):
        if self._has_retrieved:
            return self.retrivee._cached_output
        else:
            return self.retriver
        
    # extract outputs from other tasks as the inputs of this particular task
    # and do the input and output files declaration in superclass
    def submit_finalize(self, manager):
        new_args = []
        for arg in self._event['fn_args']:
            if isinstance(arg, FutureFunctionCall):
                new_args.append(arg.output)
            else:
                new_args.append(arg)
        self._event['fn_args'] = tuple(new_args)
        super().submit_finalize(manager)
        
    def __del__(self):
        super().__del__()
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
