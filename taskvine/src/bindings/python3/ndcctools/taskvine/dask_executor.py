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
from .dask_dag import DaskVineDag, find_in_lists

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
    # @param lazy_transfer Whether to keep intermediate results only at workers (True)
    #                      or to bring back each result to the manager (False, default).
    #                      True is more IO efficient, but runs the risk of needing to
    #                      recompute results if workers are lost.
    # @param resources     A dictionary with optional keys of cores, memory and disk (MB)
    #                      to set maximum resource usage per task.
    # @param resources_mode Automatically resize allocation per task. One of 'fixed'
    #                       (use the value of 'resources' above), 'max througput',
    #                       'max' (for maximum values seen), 'min_waste', 'greedy bucketing'
    #                       or 'exhaustive bucketing'. This is done per function type in dsk.
    def get(self, dsk, keys, *,
            environment=None,
            extra_files=None,
            lazy_transfer=False,
            resources=None,
            resources_mode='fixed',
            verbose=False
            ):
        try:
            return self.dask_execute(dsk, keys,
                                     environment=environment,
                                     extra_files=extra_files,
                                     lazy_transfer=lazy_transfer,
                                     resources=resources,
                                     resources_mode=resources_mode,
                                     verbose=verbose)
        except Exception as e:
            # unhandled exceptions for now
            raise e

    def dask_execute(self, dsk, keys, *,
                     environment=None,
                     extra_files=None,
                     lazy_transfer=False,
                     resources=None,
                     resources_mode='fixed',
                     verbose=False
                     ):
        if isinstance(keys, list):
            indices = DaskVineDag.find_dask_keys(keys)
            keys_flatten = indices.keys()
        else:
            keys_flatten = [keys]

        dag = DaskVineDag(dsk)
        rs = dag.set_targets(keys_flatten)

        tag = f"dag-{id(dag)}"

        self.submit_calls(dag, tag, rs,
                          environment=environment,
                          extra_files=extra_files,
                          lazy_transfer=lazy_transfer,
                          resources=resources,
                          resources_mode=resources_mode)

        while not self.empty():
            t = self.wait_for_tag(tag, 5)
            if t:
                if t.successful():
                    if verbose:
                        print(f"{t.key} ran on {t.hostname} with result {t.output}")

                    rs = dag.set_result(t.key, DaskVineFile(t.output_file, t.key, self.staging_directory))
                    self.submit_calls(dag, tag, rs,
                                      environment=environment,
                                      extra_files=extra_files,
                                      lazy_transfer=lazy_transfer,
                                      resources=resources,
                                      resources_mode=resources_mode)
                else:
                    raise Exception(f"task for key {t.key} failed: {t.result}. exit code {t.exit_code}\n{t.output}")

        return self._load_results(dag, indices, keys)

    def submit_calls(self, dag, tag, rs, *,
                     environment=None,
                     extra_files=None,
                     lazy_transfer=False,
                     resources=None,
                     resources_mode=None,
                     ):
        resources_already_set = set()
        targets = dag.get_targets()
        for r in rs:
            k, (fn, *args) = r
            t = PythonTaskDask(self,
                               k, fn, args,  # compute key k from fn(args)
                               environment=environment,
                               extra_files=extra_files,
                               lazy_transfer=(lazy_transfer and k not in targets))

            t.set_tag(tag)  # tag that identifies this dag

            cat = str(fn)
            if cat not in resources_already_set:
                if resources_mode:
                    self.set_category_mode(cat, resources_mode)
                    self.set_category_resources_max(cat, resources)
                    self.enable_monitoring()

            self.submit(t)

    def _load_results(self, dag, indices, keys):
        results = list(keys)
        for k, ids in indices.items():
            r = dag.get_result(k)
            if isinstance(r, DaskVineFile):
                r = r.load()
            set_at_indices(results, ids, r)
        if not isinstance(keys, list):
            return results[0]
        return results


class DaskVineFile:
    def __init__(self, file, key, staging_dir):
        self._file = file
        assert file

    def load(self):
        with open(self.staging_path, "rb") as f:
            return cloudpickle.load(f)

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
        print(self.backtrace.format_tb())
        return "hello"


class PythonTaskDask(PythonTask):
    def __init__(self, m, key, fn, args, *,
                 environment=None,
                 extra_files=None,
                 lazy_transfer=False):
        self._key = key

        file_indices = find_result_files(args)
        new_args = list(args)
        names = []
        for f, inds in file_indices.items():
            name = str(uuid4())  # choose some random name for the remote name
            set_at_indices(new_args, inds, name)
            names.append((f, name))
        super().__init__(wrap_function_load_args, file_indices.values(), fn, new_args)

        for f, name in names:
            self.add_input(f.file, name)

        self.set_category(str(fn))
        if lazy_transfer:
            self.enable_temp_output()
        if environment:
            self.add_environment(environment)
        if extra_files:
            for f, name in extra_files.items():
                self.add_input(f, name)

    @property
    def key(self):
        return self._key


# Most of the arguments are represented by files. We wrap the original call
# to load the contents of the files to the arguments.
# file_indices contains the indices of the args list of those arguments
# that should be loaded.
def wrap_function_load_args(indices, fn, args):
    import traceback
    import cloudpickle
    loaded_args = list(args)

    for inds in indices:
        name = get_at_indices(loaded_args, inds)
        with open(name, "rb") as f:
            set_at_indices(loaded_args, inds, cloudpickle.load(f))
    try:
        return fn(*loaded_args)
    except Exception:
        raise DaskVineExecutionError(traceback.extract_stack())


def set_at_indices(lst, indices, value):
    inner = lst
    for i in indices[:-1]:
        inner = inner[i]
    inner[indices[-1]] = value


def get_at_indices(lst, indices):
    inner = lst
    for i in indices[:-1]:
        inner = inner[i]
    return inner[indices[-1]]


def find_result_files(lists):
    return find_in_lists(lists, lambda f: isinstance(f, DaskVineFile))
