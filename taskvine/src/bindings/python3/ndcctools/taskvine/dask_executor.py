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
from .dask_dag import DaskVineDag

import cloudpickle
from uuid import uuid4

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
    # @param keys          A single key or a possible nested list of keys to compute the
    #                      value from dsk.
    # @param environment   A taskvine file representing an environment to run the tasks.
    # @param extra_files   A dictionary of {taskvine.File: "remote_name"} to add to each
    #                      task.
    # @param lazy_transfers Whether to keep intermediate results only at workers (True)
    #                      or to bring back each result to the manager (False, default).
    #                      True is more IO efficient, but runs the risk of needing to
    #                      recompute results if workers are lost.
    # @param low_memory_mode Split graph vertices to reduce memory needed per function call. It
    #                      removes some of the dask graph optimizations, thus proceed with care.
    # @param  checkpoint_fn When using lazy_transfers, a predicate with arguments (dag, key)
    #                      called before submitting a task. If True, the result is brought back
    #                      to the manager.
    # @param resources     A dictionary with optional keys of cores, memory and disk (MB)
    #                      to set maximum resource usage per task.
    # @param resources_mode Automatically resize allocation per task. One of 'fixed'
    #                       (use the value of 'resources' above), 'max througput',
    #                       'max' (for maximum values seen), 'min_waste', 'greedy bucketing'
    #                       or 'exhaustive bucketing'. This is done per function type in dsk.
    # @param retries       Number of times to attempt a task. Default is 5.
    # @param verbose       if true, emit additional debugging information.
    
    def get(self, dsk, keys, *,
            environment=None,
            extra_files=None,
            lazy_transfers=False,
            low_memory_mode=False,
            checkpoint_fn=None,
            resources=None,
            resources_mode='fixed',
            retries=5,
            verbose=False
            ):
        try:
            if retries and retries < 1:
                raise ValueError("retries should be larger than 0")

            self.environment = environment
            self.extra_files = extra_files
            self.lazy_transfers = lazy_transfers
            self.low_memory_mode = low_memory_mode
            self.checkpoint_fn = checkpoint_fn
            self.resources = resources
            self.resources_mode = resources_mode
            self.retries = retries
            self.verbose = verbose

            self._categories_known = set()

            return self._dask_execute(dsk, keys)
        except Exception as e:
            # unhandled exceptions for now
            raise e

    def _dask_execute(self, dsk, keys):
        if isinstance(keys, list):
            indices = {k: inds for (k, inds) in find_dask_keys(keys)}
            keys_flatten = indices.keys()
        else:
            keys_flatten = [keys]

        dag = DaskVineDag(dsk, low_memory_mode=self.low_memory_mode)
        tag = f"dag-{id(dag)}"

        rs = dag.set_targets(keys_flatten)
        self._submit_calls(dag, tag, rs)

        while not self.empty():
            t = self.wait_for_tag(tag, 5)
            if t:
                if self.verbose:
                    print(f"{t.key} ran on {t.hostname}")

                if t.successful():
                    rs = dag.set_result(t.key, DaskVineFile(t.output_file, t.key, self.staging_directory))
                    self._submit_calls(dag, tag, rs)
                else:
                    retries_left = t.decrement_retry()
                    print(f"task id {t.id} key {t.key} failed. {retries_left} attempts left.\n{t.std_output}")
                    if retries_left > 0:
                        self._submit_calls(dag, tag, [(t.key, t.sexpr)])
                    else:
                        raise Exception(f"tasks for key {t.key} failed permanently")
        return self._load_results(dag, indices, keys)

    def category_name(self, sexpr):
        if DaskVineDag.taskp(sexpr):
            return str(sexpr[0]).replace(" ", "_")
        else:
            return "other"

    def _submit_calls(self, dag, tag, rs):
        targets = dag.get_targets()
        for (k, sexpr) in rs:
            lazy = self.lazy_transfers and k not in targets
            if lazy and self.checkpoint_fn:
                lazy = self.checkpoint_fn(dag, k)

            cat = self.category_name(sexpr)
            if cat not in self._categories_known:
                if self.resources:
                    self.set_category_resources_max(cat, self.resources)
                if self.resources_mode:
                    self.set_category_mode(cat, self.resources_mode)

                    if not self._categories_known:
                        self.enable_monitoring()
                self._categories_known.add(cat)

            t = PythonTaskDask(self,
                               dag, k, sexpr,
                               category=cat,
                               environment=self.environment,
                               extra_files=self.extra_files,
                               retries=self.retries,
                               lazy_transfers=lazy)
            t.set_tag(tag)  # tag that identifies this dag
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
        elif isinstance(raw, DaskVineFile):
            return raw.load()
        else:
            return raw

##
# @class ndcctools.taskvine.dask_executor.DaskVineFile
#
# Internal class used to represent Python data persisted
# as a file in the filesystem.
#

class DaskVineFile:
    def __init__(self, file, key, staging_dir):
        self._file = file
        self._loaded = False
        self._load = None
        assert file

    def load(self):
        if not self._loaded:
            with open(self.staging_path, "rb") as f:
                self._load = cloudpickle.load(f)
                self._loaded = True
        return self._load

    @property
    def file(self):
        return self._file

    @property
    def staging_path(self):
        return self._file.source()


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
    # @param key   Key of task in graph.
    # @param sexpr          Positional arguments to function.
    # @param category       TaskVine category name.
    # @param environment    TaskVine execution environment.
    # @param extra_files    Additional files to provide to the task.
    # @param retries        Number of times to retry failed task.
    # @param lazy_transfers If true, do not return outputs to manager until required.
    #
    
    def __init__(self, m,
                 dag, key, sexpr, *,
                 category=None,
                 environment=None,
                 extra_files=None,
                 retries=5,
                 lazy_transfers=False):
        self._key = key
        self._sexpr = sexpr

        self._retries_left = retries

        args_raw = {k: dag.get_result(k) for k in dag.get_children(key)}
        args = {k: f"{uuid4()}.p" for k, v in args_raw.items() if isinstance(v, DaskVineFile)}

        keys_of_files = list(args.keys())
        args = args_raw | args

        super().__init__(execute_graph_vertex, sexpr, args, keys_of_files)
        self.set_output_cache(cache=True)

        for k, f in args_raw.items():
            if isinstance(f, DaskVineFile):
                self.add_input(f.file, args[k])

        if category:
            self.set_category(category)
        if lazy_transfers:
            self.enable_temp_output()
        if environment:
            self.add_environment(environment)
        if extra_files:
            for f, name in extra_files.items():
                self.add_input(f, name)

    @property
    def key(self):
        return self._key

    @property
    def sexpr(self):
        return self._sexpr

    def decrement_retry(self):
        self._retries_left -= 1
        return self._retries_left


def execute_graph_vertex(sexpr, args, keys_of_files):
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
        return rec_call(sexpr)
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
