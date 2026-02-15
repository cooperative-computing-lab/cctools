##
# @package ndcctools.work_queue.dask_executor
#
# This module provides a specialized manager @ref ndcctools.work_queue.dask_executor.DaskWQ to execute
# dask workflows.

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from .work_queue import WorkQueue, PythonTask, staging_directory, WORK_QUEUE_RESULT_SUCCESS
from .taskvine.dask_dag import DaskVineDag

import os
import cloudpickle
from uuid import uuid4

##
# @class ndcctools.work_queue.dask_executor.DaskWQ
#
# WorkQueue Manager specialized to compute dask graphs.
#
# Managers created via DaskWQ can be used to execute dask graphs via the method
# @ref ndcctools.work_queue.dask_executor.DaskWQ.get as follows:
#
# @code
# m = DaskWQ(...)
# # Initialize as any other. @see ndcctools.work_queue.WorkQueue
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
# with dask.config.set(scheduler=m.get):
#     # Each task uses at most 4 cores, they run in the my_env environment, and
#     # their allocation is set to maximum values seen.
#     # If resource_mode is different than None, then the resource monitor is activated.
#     result = v.compute(resources={"cores": 1}, resources_mode="max", environment="my_env.tar.gz")
# @endcode


class DaskWQ(WorkQueue):
    ##
    # Execute the task graph dsk and return the results for keys
    # in graph.
    # @param dsk           The task graph to execute.
    # @param keys          A possible nested list of keys to compute the value from dsk.
    # @param environment   A work_queue file representing an environment to run the tasks.
    # @param extra_files   A dictionary of {"local_path": "remote_name"} of files to add to each
    #                      task.
    # @param env_vars      A dictionary of VAR=VALUE environment variables to set per task. A value
    #                      should be either a string, or a function that accepts as arguments the manager
    #                      and task, and that returns a string.
    # @param low_memory_mode Split graph vertices to reduce memory needed per function call. It
    #                      removes some of the dask graph optimizations, thus proceed with care.
    # @param resources     A dictionary with optional keys of cores, memory and disk (MB)
    #                      to set maximum resource usage per task.
    # @param resources_mode One of the following allocation modes:
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_FIXED
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_MAX
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_GREEDY_BUCKETING
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_EXHAUSTIVE_BUCKETING
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_DET_GREEDY_BUCKETING
    #                      - work_queue.WORK_QUEUE_ALLOCATION_MODE_DET_EXHAUSTIVE_BUCKETING
    # @param retries       Number of times to attempt a task. Default is 5.
    # @param verbose       if true, emit additional debugging information.
    def get(self, dsk, keys, *,
            environment=None,
            extra_files=None,
            env_vars=None,
            low_memory_mode=False,
            resources=None,
            resources_mode=None,
            retries=5,
            verbose=False,
            **ignored
            ):
        try:

            if ignored:
                print("options ignored:", ignored)

            if retries and retries < 1:
                raise ValueError("retries should be larger than 0")

            if isinstance(environment, str):
                self.environment = self.declare_poncho(environment, cache=True)
            else:
                self.environment = environment

            self.extra_files = extra_files
            self.env_vars = env_vars
            self.low_memory_mode = low_memory_mode
            self.resources = resources
            self.resources_mode = resources_mode
            self.retries = retries
            self.verbose = verbose

            self._categories_known = set()

            return self._dask_execute(dsk, keys)
        except Exception as e:
            # unhandled exceptions for now
            raise e

    def __call__(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def _dask_execute(self, dsk, keys):
        indices = {k: inds for (k, inds) in find_dask_keys(keys)}
        keys_flatten = indices.keys()

        dag = DaskVineDag(dsk, low_memory_mode=self.low_memory_mode)
        tag = f"dag-{id(dag)}"

        rs = dag.set_targets(keys_flatten)
        self._submit_calls(dag, tag, rs, self.retries)

        while not self.empty():
            t = self.wait_for_tag(tag, 5)
            if t:
                if self.verbose:
                    print(f"{t.key} ran on {t.hostname}")

                if t.successful():
                    rs = dag.set_result(t.key, DaskWQFile(t.output_file, t.key))
                    self._submit_calls(dag, tag, rs, self.retries)
                else:
                    retries_left = t.decrement_retry()
                    print(f"task id {t.id} key {t.key} failed: {t.result}. {retries_left} attempts left.\n{t.std_output}")
                    if retries_left > 0:
                        self._submit_calls(dag, tag, [(t.key, t.sexpr)], retries_left)
                    else:
                        raise Exception(f"tasks for key {t.key} failed permanently")
        return self._load_results(dag, indices, keys)

    def category_name(self, sexpr):
        if DaskVineDag.taskp(sexpr):
            return str(sexpr[0]).replace(" ", "_")
        else:
            return "other"

    def _submit_calls(self, dag, tag, rs, retries):
        for (k, sexpr) in rs:
            cat = self.category_name(sexpr)
            if cat not in self._categories_known:
                if self.resources:
                    self.specify_category_max_resources(cat, self.resources)
                if self.resources_mode:
                    self.specify_category_mode(cat, self.resources_mode)

                    if not self._categories_known:
                        self.enable_monitoring()
                self._categories_known.add(cat)

            t = PythonTaskDask(self,
                               dag, k, sexpr, staging_directory,
                               category=cat,
                               environment=self.environment,
                               extra_files=self.extra_files,
                               env_vars=self.env_vars,
                               retries=retries)
            t.specify_tag(tag)  # tag that identifies this dag
            self.submit(t)

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
        if DaskVineDag.listp(raw):
            result = list(raw)
            file_indices = find_result_files(raw)
            for (f, inds) in file_indices:
                set_at_indices(result, inds, f.load())
            return result
        elif isinstance(raw, DaskWQFile):
            return raw.load()
        else:
            return raw

##
# @class ndcctools.work_queue.dask_executor.DaskWQFile
#
# Internal class used to represent Python data persisted
# as a file in the filesystem.
#


class DaskWQFile:
    def __init__(self, file, key):
        self._name = file
        self._key = key
        self._loaded = False
        self._load = None

    def load(self):
        if not self._loaded:
            with open(self._name, "rb") as f:
                self._load = cloudpickle.load(f)
                self._loaded = True
        return self._load

    @property
    def name(self):
        return self._name


##
# @class ndcctools.work_queue.dask_executor.DaskWQExecutionError
#
# Internal class representing an execution error in a WorkQueue workflow.

class DaskWQExecutionError(Exception):
    def __init__(self, backtrace):
        self.backtrace = backtrace

    def str(self):
        return self.backtrace.format_tb()


##
# @class ndcctools.work_queue.dask_executor.PythonTaskDask
#
# Internal class representing a Dask task to be executed,
# materialized as a special case of a WorkQueue PythonTask.
#

class PythonTaskDask(PythonTask):
    ##
    # Create a new PythonTaskDask.
    #
    # @param self  This task object.
    # @param m     WorkQueue manager object.
    # @param dag   Dask graph object.
    # @param key   Key of task in graph.
    # @param sexpr          Positional arguments to function.
    # @param staging_dir    Where to write output files.
    # @param category       WorkQueue category name.
    # @param environment    WorkQueue execution environment.
    # @param extra_files    Additional files to provide to the task.
    # @param env_vars       A dictionary of environment variables.
    # @param retries        Number of times to retry failed task.
    def __init__(self, m,
                 dag, key, sexpr, staging_dir, *,
                 category=None,
                 environment=None,
                 extra_files=None,
                 env_vars=None,
                 retries=5):
        self._key = key
        self._sexpr = sexpr

        self._retries_left = retries

        args_raw = {k: dag.get_result(k) for k in dag.get_children(key)}
        args = {k: f"{uuid4()}.p" for k, v in args_raw.items() if isinstance(v, DaskWQFile)}

        self.output_file = os.path.join(staging_dir, f"{uuid4()}.p")

        keys_of_files = list(args.keys())
        args = args_raw | args

        super().__init__(execute_graph_vertex, sexpr, args, keys_of_files, "pickled_output")
        self.specify_output_file(self.output_file, "pickled_output")

        for k, f in args_raw.items():
            if isinstance(f, DaskWQFile):
                self.specify_input_file(f.name, args[k])

        if category:
            self.specify_category(category)
        if environment:
            self.specify_environment(environment)
        if extra_files:
            for mgr_name, worker_name in extra_files.items():
                self.specify_input_file(mgr_name, worker_name)
        if env_vars:
            for k, v in env_vars.items():
                if callable(v):
                    s = v(m, self)
                else:
                    s = v
                self.specify_environment_variable(k, s)

    @property
    def key(self):
        return self._key

    @property
    def sexpr(self):
        return self._sexpr

    def decrement_retry(self):
        self._retries_left -= 1
        return self._retries_left

    def successful(self):
        return self.return_status == 0 and self.result == WORK_QUEUE_RESULT_SUCCESS


def execute_graph_vertex(sexpr, args, keys_of_files, output_name):
    import traceback
    import cloudpickle

    def rec_call(sexpr):
        if DaskVineDag.keyp(sexpr) and sexpr in args:
            return args[sexpr]
        elif DaskVineDag.taskp(sexpr):
            return sexpr[0](*[rec_call(a) for a in sexpr[1:]])
        elif DaskVineDag.listp(sexpr):
            return [rec_call(a) for a in sexpr]
        else:
            return sexpr

    for k in keys_of_files:
        try:
            with open(args[k], "rb") as f:
                args[k] = cloudpickle.load(f)
        except Exception as e:
            print(f"Could not read input file {args[k]} for key {k}: {e}")
            raise

    try:
        result = rec_call(sexpr)
        with open(output_name, "wb") as f:
            cloudpickle.dump(result, f)
        return output_name
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
    return find_in_lists(lists, lambda f: isinstance(f, DaskWQFile))
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
