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
    # param  checkpoint_fn When using lazy_transfers, a predicate with arguments (dag, key)
    #                      called before submitting a task. If True, the result is brought back
    #                      to the manager.
    # @param resources     A dictionary with optional keys of cores, memory and disk (MB)
    #                      to set maximum resource usage per task.
    # @param resources_mode Automatically resize allocation per task. One of 'fixed'
    #                       (use the value of 'resources' above), 'max througput',
    #                       'max' (for maximum values seen), 'min_waste', 'greedy bucketing'
    #                       or 'exhaustive bucketing'. This is done per function type in dsk.
    def get(self, dsk, keys, *,
            environment=None,
            extra_files=None,
            lazy_transfers=False,
            low_memory_mode=False,
            checkpoint_fn=None,
            resources=None,
            resources_mode='fixed',
            verbose=False
            ):
        try:
            self._categories_known = set()
            return self.dask_execute(dsk, keys,
                                     environment=environment,
                                     extra_files=extra_files,
                                     lazy_transfers=lazy_transfers,
                                     checkpoint_fn=checkpoint_fn,
                                     resources=resources,
                                     resources_mode=resources_mode,
                                     verbose=verbose)
        except Exception as e:
            # unhandled exceptions for now
            raise e

    def dask_execute(self, dsk, keys, *,
                     environment=None,
                     extra_files=None,
                     lazy_transfers=False,
                     checkpoint_fn=None,
                     resources=None,
                     resources_mode='fixed',
                     low_memory_mode=False,
                     verbose=False
                     ):
        if isinstance(keys, list):
            indices = {k: inds for (k, inds) in find_dask_keys(keys)}
            keys_flatten = indices.keys()
        else:
            keys_flatten = [keys]

        dag = DaskVineDag(dsk, low_memory_mode=low_memory_mode)
        rs = dag.set_targets(keys_flatten)

        tag = f"dag-{id(dag)}"

        self.submit_calls(dag, tag, rs,
                          environment=environment,
                          extra_files=extra_files,
                          lazy_transfers=lazy_transfers,
                          checkpoint_fn=checkpoint_fn,
                          resources=resources,
                          resources_mode=resources_mode)

        while not self.empty():
            t = self.wait_for_tag(tag, 5)
            if t:
                if verbose:
                    print(f"{t.key} ran on {t.hostname}")

                if t.successful():
                    rs = dag.set_result(t.key, DaskVineFile(t.output_file, t.key, self.staging_directory))
                    self.submit_calls(dag, tag, rs,
                                      environment=environment,
                                      extra_files=extra_files,
                                      lazy_transfers=lazy_transfers,
                                      checkpoint_fn=checkpoint_fn,
                                      resources=resources,
                                      resources_mode=resources_mode)
                else:
                    Exception(f"task for key {t.key} failed. exit code {t.exit_code}\n{t.std_output}")

        return self._load_results(dag, indices, keys)

    def category_name(self, sexpr):
        if DaskVineDag.taskp(sexpr):
            return str(sexpr[0]).replace(" ", "_")
        else:
            return "other"

    def submit_calls(self, dag, tag, rs, *,
                     environment=None,
                     extra_files=None,
                     lazy_transfers=False,
                     checkpoint_fn=None,
                     resources=None,
                     resources_mode=None,
                     ):

        targets = dag.get_targets()
        for (k, sexpr) in rs:
            lazy = lazy_transfers and k not in targets
            if lazy and checkpoint_fn:
                lazy = checkpoint_fn(dag, k)

            cat = self.category_name(sexpr)
            if cat not in self._categories_known:
                if resources:
                    self.set_category_resources_max(cat, resources)
                if resources_mode:
                    self.set_category_mode(cat, resources_mode)

                    if not self._categories_known:
                        self.enable_monitoring()
                self._categories_known.add(cat)

            t = PythonTaskDask(self,
                               dag, k, sexpr,
                               category=cat,
                               environment=environment,
                               extra_files=extra_files,
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


class DaskVineExecutionError(Exception):
    def __init__(self, backtrace):
        self.backtrace = backtrace

    def str(self):
        return self.backtrace.format_tb()


class PythonTaskDask(PythonTask):
    def __init__(self, m,
                 dag, key, sexpr, *,
                 category=None,
                 environment=None,
                 extra_files=None,
                 lazy_transfers=False):
        self._key = key

        args_raw = {k: dag.get_result(k) for k in dag.get_children(key)}
        args = {k: f"{uuid4()}.p" for k, v in args_raw.items() if isinstance(v, DaskVineFile)}

        keys_of_files = list(args.keys())
        args = args_raw | args

        super().__init__(execute_graph_vertex, sexpr, args, keys_of_files)

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

    try:
        for k in keys_of_files:
            with open(args[k], "rb") as f:
                args[k] = cloudpickle.load(f)

        return rec_call(sexpr)
    except Exception:
        return DaskVineExecutionError(traceback.format_exc())


def set_at_indices(lst, indices, value):
    inner = lst
    for i in indices[:-1]:
        inner = inner[i]
    try:
        inner[indices[-1]] = value
    except IndexError:
        raise


def find_in_lists(lists, predicate=lambda s: True, indices=None):
    """ Returns a list of tuples [(k, (i,j,...)] where (i,j,...) are the indices of k in lists. """
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
