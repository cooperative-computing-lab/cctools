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

import os
import cloudpickle
from uuid import uuid4


##
# @class ndcctools.taskvine.dask_executor.DaskVine
#
# TaskVine Manager specialized to compute dask graphs.
#
# Managers created via DaskVine can be used to execute dask graphs via the method
# @ref ndcctools.taskvine.dask_executor.DaskVine.dask_execute as follows:
#
# @code
# m = DaskVine(...)
# # Initialize as any other. @see ndcctools.taskvine.manager.Manager
# result = v.compute(scheduler= m.dask_execute)
#
# # or set by temporarily as the default for dask:
# with dask.config.set(scheduler=m.dask_execute):
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
# with dask.config.set(scheduler=m.dask_execute):
#     # Each task uses at most 4 cores, they run in the my_env environment, and
#     # their allocation is set to maximum values seen.
#     # If resource_mode is different than None, then the resource monitor is activated.
#     result = v.compute(resources={"cores": 1}, resources_mode="max", environment=my_env)
# @endcode


class DaskVine(Manager):
    def submit_calls(self, targets, rs, *,
                     environment=None,
                     lazy_transfer=False,
                     resources=None,
                     resources_mode=None,
                     ):
        resources_already_set = set()
        for r in rs:
            k, (fn, *args) = r
            t = PythonTaskDask(self, k, fn, args)

            cat = str(fn)
            t.set_category(cat)

            if cat not in resources_already_set:
                if resources_mode:
                    self.set_category_mode(cat, resources_mode)
                    self.set_category_resources_max(cat, resources)
                    self.enable_monitoring()

            if lazy_transfer and k not in targets:
                t.enable_temp_output()
            if environment:
                t.add_environment(environment)

            self.submit(t)

    def get(self, dsk, keys, **kwargs):
        return self.dask_execute(dsk, keys, **kwargs)

    def dask_execute(self, dsk, keys, **kwargs):
        """Computes the values of the keys in the dask graph dsk"""

        if isinstance(keys, list):
            indices = DaskVineDag.find_dask_keys(keys)
            keys_flatten = indices.keys()
        else:
            keys_flatten = [keys]

        dag = DaskVineDag(dsk)
        rs = dag.set_targets(keys_flatten)

        verbose = kwargs.pop("verbose", False)

        self.submit_calls(keys_flatten, rs, **kwargs)

        while not self.empty():
            t = self.wait(5)
            if t:
                if t.successful():
                    if verbose:
                        print(f"{t.key} ran on {t.hostname} with result {t.output}")
                    rs = dag.set_result(t.key, DaskVineFile(t.output_file, t.key, self.staging_directory))
                    self.submit_calls(keys, rs, **kwargs)
                else:
                    raise Exception(f"task for key {t.key} failed: {t.result}. exit code {t.exit_code}\n{t.output}")

        return self._load_results(dag, indices, keys)

    def _load_results(self, dag, indices, keys):
        results = list(keys)
        for k, ids in indices.items():
            r = dag.get_result(k)
            if isinstance(r, DaskVineFile):
                r = r.load()
            DaskVineDag.set_dask_result(results, ids, r)
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

    @property
    def basename(self):
        return os.path.basename(self.staging_path)


class PythonTaskDask(PythonTask):
    def __init__(self, m, key, fn, args):
        self._key = key

        file_indices = []
        new_args = []
        for i, a in enumerate(args):
            if isinstance(a, DaskVineFile):
                file_indices.append(i)
                a = str(uuid4())  # choose some random name for the remote name
            new_args.append(a)
        super().__init__(wrap_function_load_args, file_indices, fn, new_args)

        for i in file_indices:
            f = args[i]
            self.add_input(f.file, new_args[i])

    @property
    def key(self):
        return self._key


# Most of the arguments are represented by files. We wrap the original call
# to load the contents of the files to the arguments.
# file_indices contains the indices of the args list of those arguments
# that should be loaded.
def wrap_function_load_args(file_indices, fn, args):
    import cloudpickle
    loaded_args = list(args)
    for i in file_indices:
        with open(args[i], "rb") as f:
            loaded_args[i] = cloudpickle.load(f)
    return fn(*loaded_args)
