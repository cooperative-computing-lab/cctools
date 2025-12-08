##
# @package ndcctools.taskvine.dask_executor
#
# This module provides a specialized manager @ref ndcctools.taskvine.dask_executor.DaskVine to execute
# dask workflows.

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from .manager import Manager
from .task import PythonTask
from .task import FunctionCall
from .dask_dag import DaskVineDag
from .cvine import VINE_TEMP

try:
    import dask._task_spec as dts
except ModuleNotFoundError:
    print("Dask version 2024.12.0 or greater not found.")
    raise


import os
import time
import random
import contextlib
import cloudpickle
from uuid import uuid4
from collections import defaultdict

try:
    import rich
    from rich import print
except ImportError:
    rich = None


def daskvine_merge(func):
    def flex_merge(*args, **kwargs):
        result = func(*args, **kwargs)
        return result
    return flex_merge

##
# @class ndcctools.taskvine.dask_executor.DaskVine
#
# TaskVine Manager specialized to compute dask graphs.
#
# Managers created via DaskVine can be used to execute dask graphs via the method
# @ref ndcctools.taskvine.dask_executor.DaskVine.get as follows:
#
# @code
# m = DaskVine(...)
# # Initialize as any other. @see ndcctools.taskvine.manager.Manager
# result = v.compute(scheduler= m.get)
#
# # or set by temporarily as the default for dask:
# with dask.config.set(scheduler=m.get):
#     result = v.compute()
# @endcode
#
# Parameters for execution can be set as arguments to the compute function. These
# arguments are applied to each task executed:
#
# @code
#
# my_env = m.declare_poncho("my_env.tar.gz")
#
# with dask.config.set(scheduler=m.get):
#     # Each task uses at most 4 cores, they run in the my_env environment, and
#     # their allocation is set to maximum values seen.
#     # If resource_mode is different than None, then the resource monitor is activated.
#     result = v.compute(resources={"cores": 1}, resources_mode="max", environment=my_env)
# @endcode


class DaskVine(Manager):
    ##
    # Execute the task graph dsk and return the results for keys
    # in graph.
    # @param dsk           The task graph to execute.
    # @param keys          A possible nested list of keys to compute the value from dsk.
    # @param environment   A taskvine file representing an environment to run the tasks.
    # @param extra_files   A dictionary of {taskvine.File: "remote_name"} to add to each
    #                      task.
    # @param worker_transfers Whether to keep intermediate results only at workers (True, default)
    #                      or to bring back each result to the manager (False).
    #                      True is more IO efficient, but runs the risk of needing to
    #                      recompute results if workers are lost.
    # @param env_vars      A dictionary of VAR=VALUE environment variables to set per task. A value
    #                      should be either a string, or a function that accepts as arguments the manager
    #                      and task, and that returns a string.
    # @param checkpoint_fn When using worker_transfers, a predicate with arguments (dag, key)
    #                      called before submitting a task. If True, the result is brought back
    #                      to the manager.
    # @param resources     A dictionary with optional keys of cores, memory and disk (MB)
    #                      to set maximum resource usage per task.
    # @param lib_extra_functions Additional functions to include in execution library (function-calls task_mode)
    # @param lib_resources A dictionary with optional keys of cores, memory and disk in MB (function-calls task_mode)
    # @param lib_command A command to be prefixed to the execution of a Library task (function-calls task_mode)
    # @param lib_modules Hoist these module imports for the execution library (function-calls task_mode)
    # @param env_per_task execute each task
    # @param resources_mode Automatically resize allocation per task. One of 'fixed'
    #                       (use the value of 'resources' above), 'max througput',
    #                       'max' (for maximum values seen), 'min_waste', 'greedy bucketing',
    #                       'exhaustive bucketing', 'deterministic greedy bucketing', or
    #                       'deterministic exhaustive bucketing'.
    #                       This is done per function type in dsk.
    # @param task_mode     Create tasks as either as 'tasks' (using PythonTasks) or 'function-calls' (using FunctionCalls)
    # @param retries       Number of times to attempt a task. Default is 5.
    # @param submit_per_cycle Maximum number of tasks to submit to scheduler at once. If None, or less than 1, then all
    #                         tasks are submitted as they are available.
    # @param max_pending   Maximum number of tasks without a result before new ones are submitted to the scheduler.
    #                      If None, or less than 1, then no limit is set.
    # @param verbose       if true, emit additional debugging information.
    # @param env_per_task  if true, each task individually expands its own environment. Must use environment option as a str.
    # @param progress_disable If True, disable progress bar
    # @param progress_label Label to use in progress bar
    # @param reconstruct   Reconstruct graph based on annotated functions.
    # @param merge_size    When reconstructing a merge function, merge this many at a time
    # @param wrapper       Function to wrap dask calls. It should take as arguments (key, fn, *args). It should execute
    #                      fn(*args) at some point during its execution to produce the dask task result.
    #                      Should return a tuple of (wrapper result, dask call result). Use for debugging.
    # @param wrapper_proc  Function to process results from wrapper on completion. (default is print)
    # @param prune_depth Control pruning behavior: 0 (default) - no pruning, 1 - only check direct consumers, 2+ - check consumers up to specified depth
    def get(self, dsk, keys, *,
            environment=None,
            extra_files=None,
            worker_transfers=True,
            env_vars=None,
            checkpoint_fn=None,
            resources=None,
            resources_mode=None,
            submit_per_cycle=None,
            max_pending=None,
            retries=5,
            verbose=False,
            lib_extra_functions=None,
            lib_resources=None,
            lib_command=None,
            lib_modules=None,
            task_mode='tasks',
            scheduling_mode='FIFO',
            env_per_task=False,
            progress_disable=False,
            reconstruct=False,
            merge_size=2,
            progress_label="[green]tasks",
            wrapper=None,
            wrapper_proc=print,
            prune_depth=0,
            hoisting_modules=None,  # Deprecated, use lib_modules
            import_modules=None,    # Deprecated, use lib_modules
            lazy_transfers=True,    # Deprecated, use worker_tranfers
            ):
        try:

            # just in case, convert any tuple task to dts.Task
            dsk = dts.convert_legacy_graph(dsk)

            self.set_property("framework", "dask")
            if retries and retries < 1:
                raise ValueError("retries should be larger than 0")

            if isinstance(environment, str):
                self.environment = self.declare_poncho(environment, cache=True)
            else:
                self.environment = environment

            self.extra_files = extra_files
            # if one of both is False, then worker_transfers is False, otherwise True
            if not worker_transfers or not lazy_transfers:
                self.worker_transfers = False
            else:
                self.worker_transfers = True
            self.env_vars = env_vars
            self.checkpoint_fn = checkpoint_fn
            self.resources = resources
            self.resources_mode = resources_mode
            self.retries = retries
            self.verbose = verbose
            self.lib_extra_functions = lib_extra_functions
            self.lib_resources = lib_resources
            self.lib_command = lib_command
            if lib_modules:
                self.lib_modules = lib_modules
            else:
                self.lib_modules = hoisting_modules if hoisting_modules else import_modules  # Deprecated
            self.task_mode = task_mode
            self.scheduling_mode = scheduling_mode
            self.env_per_task = env_per_task
            self.reconstruct = reconstruct
            self.merge_size = merge_size
            self.progress_disable = progress_disable
            self.progress_label = progress_label
            self.wrapper = wrapper
            self.wrapper_proc = wrapper_proc
            self.prune_depth = prune_depth
            self.category_info = defaultdict(lambda: {"num_tasks": 0, "total_execution_time": 0})
            self.max_priority = float('inf')
            self.min_priority = float('-inf')

            if submit_per_cycle is not None and submit_per_cycle < 1:
                submit_per_cycle = None
            self.submit_per_cycle = submit_per_cycle

            if max_pending is not None and max_pending < 1:
                max_pending = None
            self.max_pending = max_pending

            self._categories_known = set()

            self.tune("category-steady-n-tasks", 1)
            self.tune("prefer-dispatch", 1)
            self.tune("immediate-recovery", 1)

            if self.env_per_task:
                self.environment_file = self.declare_file(environment, cache=True)
                self.environment_name = os.path.basename(environment)
                self.environment = None

            return self._dask_execute(dsk, keys)
        except Exception as e:
            # unhandled exceptions for now
            raise e
        finally:
            self.update_catalog()

    def __call__(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def _dask_execute(self, dsk, keys):
        indices = {k: inds for (k, inds) in find_dask_keys(keys)}
        keys_flatten = indices.keys()

        dag = DaskVineDag(dsk, reconstruct=self.reconstruct, merge_size=self.merge_size, prune_depth=self.prune_depth)

        tag = f"dag-{id(dag)}"

        # create Library if using 'function-calls' task mode.
        if self.task_mode == 'function-calls':
            functions = [execute_graph_vertex]
            if self.lib_extra_functions:
                functions.extend(self.lib_extra_functions)
            libtask = self.create_library_from_functions(f'Dask-Library-{id(dag)}',
                                                         *functions,
                                                         poncho_env="dummy-value",
                                                         add_env=False,
                                                         init_command=self.lib_command,
                                                         hoisting_modules=self.lib_modules)

            if self.environment:
                libtask.add_environment(self.environment)

            if self.lib_resources:
                if 'cores' in self.lib_resources:
                    libtask.set_cores(self.lib_resources['cores'])
                    libtask.set_function_slots(self.lib_resources['cores'])  # use cores as  fallback for slots
                if 'memory' in self.lib_resources:
                    libtask.set_memory(self.lib_resources['memory'])
                if 'disk' in self.lib_resources:
                    libtask.set_disk(self.lib_resources['disk'])
                if 'slots' in self.lib_resources:
                    libtask.set_function_slots(self.lib_resources['slots'])
                if self.env_vars:
                    for k, v in self.env_vars.items():
                        if callable(v):
                            s = v(self, libtask)
                        else:
                            s = v
                        libtask.set_env_var(k, s)
                if self.extra_files:
                    for f, name in self.extra_files.items():
                        libtask.add_input(f, name)

            self.install_library(libtask)

        if self.reconstruct:
            keys_flatten = dag.replace_targets(keys_flatten)

        enqueued_calls = []
        rs = dag.set_targets(keys_flatten)
        self._enqueue_dask_calls(dag, tag, rs, self.retries, enqueued_calls)

        timeout = 5
        pending = 0
        (bar_progress, bar_update) = self._make_progress_bar(dag.left_to_compute())
        with bar_progress:
            while not self.empty() or enqueued_calls:
                submitted = 0
                while (
                    enqueued_calls
                    and (not self.submit_per_cycle or submitted < self.submit_per_cycle)
                    and (not self.max_pending or pending < self.max_pending)
                    and self.hungry()
                ):
                    t = enqueued_calls.pop()
                    self.submit(t)
                    submitted += 1
                    pending += 1

                t = self.wait_for_tag(tag, timeout)
                if t:
                    timeout = 0
                    pending -= 1
                    if self.verbose:
                        print(f"{t.key} ran on {t.hostname}")

                    if t.successful():
                        self.category_info[t.category]["num_tasks"] += 1
                        self.category_info[t.category]["total_execution_time"] += t.resources_measured.wall_time
                        result_file = DaskVineFile(t.output_file, t.key, dag, self.task_mode)
                        rs = dag.set_result(t.key, result_file)
                        self._enqueue_dask_calls(dag, tag, rs, self.retries, enqueued_calls)

                        if self.wrapper:
                            self.wrapper_proc(t.load_wrapper_output(self))

                        if t.key in dsk:
                            bar_update(advance=1)

                        if self.prune_depth > 0:
                            for p in dag.pending_producers[t.key]:
                                dag.pending_consumers[p] -= 1
                                if dag.pending_consumers[p] == 0:
                                    p_result = dag.get_result(p)
                                    self.prune_file(p_result._file)

                    else:
                        retries_left = t.decrement_retry()
                        print(f"task id {t.id} key {t.key} failed: {t.result}. {retries_left} attempts left.\n{t.std_output}")
                        if retries_left > 0:
                            self._enqueue_dask_calls(dag, tag, {t.key: t.dask_task}, retries_left, enqueued_calls)
                        else:
                            raise Exception(f"tasks for key {t.key} failed permanently")
                    t = None  # drop task reference
                else:
                    timeout = 5
            return self._load_results(dag, indices, keys)

    def _make_progress_bar(self, total):
        if rich:
            from rich.progress import Progress, TextColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn

            progress = Progress(
                TextColumn(self.progress_label),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                disable=self.progress_disable,
            )
            graph_bar = progress.add_task(self.progress_label, total=total)

            def update(*args, **kwargs):
                progress.update(graph_bar, *args, **kwargs)

        else:
            progress = contextlib.nullcontext()

            def update(*args, **kwargs):
                pass

        return (progress, update)

    def category_name(self, node):
        if DaskVineDag.taskp(node):
            return str(node.func).replace(" ", "_")
        else:
            return "other"

    def _task_priority(self, dag, cat, key):
        task_depth = dag.depth_of(key)

        if self.scheduling_mode == "random":
            priority = random.randint(self.min_priority, self.max_priority)
        elif self.scheduling_mode == "depth-first":
            # dig more information about different kinds of tasks
            priority = task_depth
        elif self.scheduling_mode == "breadth-first":
            # prefer to start all branches as soon as possible
            priority = -task_depth
        elif self.scheduling_mode == "longest-category-first":
            # if no tasks have been executed in this category, set a high priority
            # so that we know more information about each category
            if self.category_info[cat]["num_tasks"]:
                priority = (
                    self.category_info[cat]["total_execution_time"]
                    / self.category_info[cat]["num_tasks"]
                )
            else:
                priority = self.max_priority
        elif self.scheduling_mode == "shortest-category-first":
            # if no tasks have been executed in this category, set a high priority
            # so that we know more information about each category
            if self.category_info[cat]["num_tasks"]:
                priority = (
                    -self.category_info[cat]["total_execution_time"]
                    / self.category_info[cat]["num_tasks"]
                )
            else:
                priority = self.max_priority
        elif self.scheduling_mode == "FIFO":
            # first in first out, the default behavior
            priority = -round(time.time(), 6)
        elif self.scheduling_mode == "LIFO":
            # last in first out, the opposite of FIFO
            priority = round(time.time(), 6)
        elif self.scheduling_mode == "largest-input-first":
            # best for saving disk space (with pruing)
            priority = sum([len(dag.get_result(c)._file) for c in dag.get_dependencies(key)])
        else:
            raise ValueError(f"Unknown scheduling mode {self.scheduling_mode}")

        return priority

    def _enqueue_dask_calls(self, dag, tag, rs, retries, enqueued_calls):
        targets = dag.get_targets()
        for dask_task in rs.values():
            k = dask_task.key
            lazy = self.worker_transfers and k not in targets
            if lazy and self.checkpoint_fn:
                lazy = self.checkpoint_fn(dag, k)

            # each task has a category name
            cat = self.category_name(dask_task)
            priority = self._task_priority(dag, cat, k)

            if self.task_mode == 'tasks':
                if cat not in self._categories_known:
                    if self.resources:
                        self.set_category_resources_max(cat, self.resources)
                    if self.resources_mode:
                        self.set_category_mode(cat, self.resources_mode)

                        if not self._categories_known:
                            self.enable_monitoring()
                    self._categories_known.add(cat)

                t = PythonTaskDask(self,
                                   dag, dask_task,
                                   category=cat,
                                   environment=self.environment,
                                   extra_files=self.extra_files,
                                   env_vars=self.env_vars,
                                   retries=retries,
                                   worker_transfers=lazy,
                                   wrapper=self.wrapper)

                t.set_priority(priority)
                if self.env_per_task:
                    t.set_command(
                        f"mkdir envdir && tar -xf {self._environment_name} -C envdir && envdir/bin/run_in_env {t._command}"
                    )
                    t.add_input(self.environment_file, self.environment_name)

                t.set_tag(tag)  # tag that identifies this dag
                enqueued_calls.append(t)

            if self.task_mode == 'function-calls':
                t = FunctionCallDask(self,
                                     dag, dask_task,
                                     category=cat,
                                     extra_files=self.extra_files,
                                     retries=retries,
                                     worker_transfers=lazy,
                                     wrapper=self.wrapper)

                t.set_priority(priority)
                t.set_tag(tag)  # tag that identifies this dag

                enqueued_calls.append(t)

    def _load_results(self, dag, key_indices, keys):
        results = list(keys)
        for k, ids in key_indices.items():
            r = self._fill_key_result(dag, k)
            set_at_indices(results, ids, r)
        if not isinstance(keys, list):
            return results[0]
        return results

    def _fill_key_result(self, dag, key):
        raw = dag.get_result(key)
        if DaskVineDag.containerp(raw):
            result = list(raw)
            file_indices = find_result_files(raw)
            for (f, inds) in file_indices:
                set_at_indices(result, inds, f.load())
            return result
        elif isinstance(raw, DaskVineFile):
            return raw.load()
        else:
            return raw

    def _prune_file(self, dag, key):
        children = dag.get_dependencies(key)
        for c in children:
            if len(dag.get_pending_needed_by(c)) == 0:
                c_result = dag.get_result(c)
                self.prune_file(c_result._file)

##
# @class ndcctools.taskvine.dask_executor.DaskVineFile
#
# Internal class used to represent Python data persisted
# as a file in the filesystem.
#


class DaskVineFile:
    def __init__(self, file, key, dag, task_mode):
        self._file = file
        self._key = key
        self._loaded = False
        self._load = None
        self._task_mode = task_mode
        self._dag = dag
        self._is_target = key in dag.get_targets()

        self._checkpointed = False

        assert file

    def load(self):
        if not self._loaded:
            self._load = self._file.contents(cloudpickle.load)
            if self._task_mode == 'function-calls':
                if self._load['Success']:
                    self._load = self._load['Result']
                else:
                    self._load = self._load['Reason']
            self._loaded = True
        return self._load

    @property
    def file(self):
        return self._file

    def is_temp(self):
        return self._file.type() == VINE_TEMP

    def ready_for_gc(self):
        # file on disk ready to be gc if the keys that needed as an input for computation are themselves ready.
        if not self._ready_for_gc:
            self._ready_for_gc = all(f.ready_for_gc() for f in self._dag.get_needed_by().values())
        return self._ready_for_gc


##
# @class ndcctools.taskvine.dask_executor.DaskVineExecutionError
#
# Internal class representing an execution error in a TaskVine workflow.

class DaskVineExecutionError(Exception):
    def __init__(self, backtrace):
        self.backtrace = backtrace

    def str(self):
        return self.backtrace.format_tb()


##
# @class ndcctools.taskvine.dask_executor.PythonTaskDask
#
# Internal class representing a Dask task to be executed,
# materialized as a special case of a TaskVine PythonTask.
#

class PythonTaskDask(PythonTask):
    ##
    # Create a new PythonTaskDask.
    #
    # @param self  This task object.
    # @param m     TaskVine manager object.
    # @param dag   Dask graph object.
    # @param dask_task  Dask task encoding a computation.
    # @param category       TaskVine category name.
    # @param environment    TaskVine execution environment.
    # @param extra_files    Additional files to provide to the task.
    # @param env_vars       A dictionary of environment variables.
    # @param retries        Number of times to retry failed task.
    # @param worker_transfers If true, do not return outputs to manager until required.
    # @param wrapper
    #
    def __init__(self, m,
                 dag, dask_task, *,
                 category=None,
                 environment=None,
                 extra_files=None,
                 env_vars=None,
                 retries=5,
                 worker_transfers=False,
                 wrapper=None):
        self._dask_task = dask_task

        self._retries_left = retries
        self._wrapper_output_file = None
        self._wrapper_output = None

        args_raw = {k: dag.get_result(k) for k in dag.get_dependencies(self.key)}
        args = {
            k: f"{uuid4()}.p"
            for k, v in args_raw.items()
            if isinstance(v, DaskVineFile)
        }

        keys_of_files = list(args.keys())
        args = args_raw | args

        super().__init__(execute_graph_vertex, wrapper, dask_task, args, keys_of_files)
        if wrapper:
            wo = m.declare_buffer()
            self.add_output(wo, "wrapper.output")
            self._wrapper_output_file = wo

        self.set_output_cache(cache=True)

        for k, f in args_raw.items():
            if isinstance(f, DaskVineFile):
                self.add_input(f.file, args[k])

        if category:
            self.set_category(category)
        if worker_transfers:
            self.enable_temp_output()
        if environment:
            self.add_environment(environment)
        if extra_files:
            for f, name in extra_files.items():
                self.add_input(f, name)
        if env_vars:
            for k, v in env_vars.items():
                if callable(v):
                    s = v(m, self)
                else:
                    s = v
                self.set_env_var(k, s)

    @property
    def key(self):
        return self._dask_task.key

    @property
    def dask_task(self):
        return self._dask_task

    def decrement_retry(self):
        self._retries_left -= 1
        return self._retries_left

    def set_output_name(self, filename):
        self._out_name_file = filename

    def load_wrapper_output(self, manager):
        if not self._wrapper_output:
            if self._wrapper_output_file:
                self._wrapper_output = self._wrapper_output_file.contents(cloudpickle.load)
                manager.undeclare_file(self._wrapper_output_file)
                self._wrapper_output_file = None
        return self._wrapper_output

##
# @class ndcctools.taskvine.dask_executor.FunctionCallDask
#
# Internal class representing a Dask function call to be executed,
# materialized as a special case of a TaskVine FunctionCall.
#


class FunctionCallDask(FunctionCall):
    ##
    # Create a new PythonTaskDask.
    #
    # @param self  This task object.
    # @param m     TaskVine manager object.
    # @param dag   Dask graph object.
    # @param dask_task  Dask task encoding a computation.
    # @param sexpr          Positional arguments to function.
    # @param category       TaskVine category name.
    # @param resources      Resources to be set for a FunctionCall.
    # @param extra_files    Additional files to provide to the task.
    # @param retries        Number of times to retry failed task.
    # @param worker_transfers If true, do not return outputs to manager until required.
    #

    def __init__(self, m,
                 dag, dask_task, *,
                 category=None,
                 resources=None,
                 extra_files=None,
                 retries=5,
                 worker_transfers=False,
                 wrapper=None):

        self._dask_task = dask_task
        self.resources = resources

        self._retries_left = retries
        args_raw = {k: dag.get_result(k) for k in dag.get_dependencies(self.key)}
        args = {k: f"{uuid4()}.p" for k, v in args_raw.items() if isinstance(v, DaskVineFile)}

        keys_of_files = list(args.keys())
        args = args_raw | args

        self._wrapper_output_file = None
        self._wrapper_output = None

        super().__init__(f'Dask-Library-{id(dag)}', 'execute_graph_vertex', wrapper, dask_task, args, keys_of_files)
        if wrapper:
            wo = m.declare_buffer()
            self.add_output(wo, "wrapper.output")
            self._wrapper_output_file = wo

        self.set_output_cache(cache=True)

        for k, f in args_raw.items():
            if isinstance(f, DaskVineFile):
                self.add_input(f.file, args[k])

        if category:
            self.set_category(category)
        if worker_transfers:
            self.enable_temp_output()

        if extra_files:
            for f, name in extra_files.items():
                self.add_input(f, name)

    @property
    def key(self):
        return self.dask_task.key

    @property
    def dask_task(self):
        return self._dask_task

    def decrement_retry(self):
        self._retries_left -= 1
        return self._retries_left

    def set_output_name(self, filename):
        self._out_name_file = filename

    def load_wrapper_output(self, manager):
        if not self._wrapper_output:
            if self._wrapper_output_file:
                self._wrapper_output = self._wrapper_output_file.contents(cloudpickle.load)
                manager.undeclare_file(self._wrapper_output_file)
                self._wrapper_output_file = None
        return self._wrapper_output


def execute_graph_vertex(wrapper, dask_task, task_args, keys_of_files):
    import traceback
    import cloudpickle

    for key in keys_of_files:
        filename = task_args[key]
        try:
            with open(filename, "rb") as f:
                arg = cloudpickle.load(f)
                if isinstance(arg, dict) and 'Result' in arg and arg['Result'] is not None:
                    arg = arg['Result']
                task_args[key] = arg
        except Exception as e:
            print(f"Could not read input file {filename} for key {key}: {e}")
            raise

    try:
        if wrapper:
            try:
                wrapper_result = None
                (wrapper_result, call_result) = wrapper(dask_task, task_args)
                return call_result
            except Exception:
                print(f"Wrapped call for {dask_task} failed.")
                raise
            finally:
                try:
                    with open("wrapper.output", "wb") as f:
                        cloudpickle.dump(wrapper_result, f)
                except Exception:
                    print(f"Wrapped call for {dask_task.key} failed to write output.")
                    raise
        else:
            return dask_task(task_args)
    except Exception:
        print(traceback.format_exc())
        raise


def set_at_indices(lst, indices, value):
    inner = lst
    for i in indices[:-1]:
        inner = inner[i]
    try:
        inner[indices[-1]] = value
    except IndexError:
        raise


# Returns a list of tuples [(k, (i,j,...)] where (i,j,...) are the indices of k in lists.
def find_in_lists(lists, predicate=lambda s: True, indices=None):
    if not indices:
        indices = []

    items = []
    if predicate(lists):  # e.g., lists aren't lists, but a single element
        items.append((lists, list(indices)))
    elif isinstance(lists, list):
        indices.append(0)
        for s in lists:
            items.extend(find_in_lists(s, predicate, indices))
            indices[-1] += 1
        indices.pop()
    return items


def find_dask_keys(lists):
    return find_in_lists(lists, DaskVineDag.keyp)


def find_result_files(lists):
    return find_in_lists(lists, lambda f: isinstance(f, DaskVineFile))
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
