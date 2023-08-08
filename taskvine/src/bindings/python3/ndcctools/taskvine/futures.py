
from . import cvine
from concurrent.futures import Executor
from concurrent.futures import Future
from ndcctools.resource_monitor import (
    rmsummary_delete,
    rmsummary_create,
    rmsummaryArray_getitem,
    delete_rmsummaryArray,
)

from .display import JupyterDisplay
from .file import File
from .task import (
    FunctionCall,
    LibraryTask,
    PythonTask,
    Task,
)
from .manager import (
    Factory,
    Manager,
)
from .utils import (
    set_port_range,
    get_c_constant,
)

import atexit
import errno
import itertools
import json
import math
import os
import pathlib
import shutil
import subprocess
import sys
import textwrap
import tempfile
import time
import weakref

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
        if manager_name:
            self.manager.set_name(manager_name)
        if factory:
            self.factory = Factory(batch_type=batch_type, manager=manager, manager_host_port=manager_host_port, manager_name=manager_name, 
                    factory_binary=factory_binary, worker_binary=worker_binary, log_file=os.devnull)
            self.set('min-workers', 5)
            for opt in opts:
                self.set(opt, opts[opt])
            self.factory.start()
        else: 
            self.factory = None

    def submit(self, fn, *args, **kwargs):
        if isinstance(fn, FutureTask):
            self.manager.submit(fn)
            return fn._future
        future_task = FutureTask(self.manager, False, fn, *args, **kwargs)
        self.manager.submit(future_task)
        return future_task._future

    def task(self, fn, *args, **kwargs):
        return FutureTask(self.manager, False, fn, *args, **kwargs)

    def set(self, name, value):
        if self.factory:
            return self.factory.__setattr__(name, value)

    def get(self):
        if self.factory:
            return self.factory.__getattr__(name)
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
# \class FutureTask
#
# TaskVine Futuretask object
#
# This class is a sublcass of PythonTask that is specialized for futures

class FutureTask(PythonTask):
    ##
    # Creates a new Future Task
    #
    # @param self 
    # @param func
    # @param args
    # @param kwargs
    def __init__(self, manager, rf, func, *args, **kwargs):
        super(FutureTask, self).__init__(func, *args, **kwargs)
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
            self._retrieval_task = FutureTask(self._module_manager, True, retrieve_output, self._future)
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


