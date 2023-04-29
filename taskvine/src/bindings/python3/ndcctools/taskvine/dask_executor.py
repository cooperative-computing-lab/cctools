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
    def _wrap(self, key, fn):
        # TODO: return backtrace, write result to files, read args from files
        def function(*args):
            try:
                result = fn(args)
            except Exception as e:
                result = e
            return (key, result)
        return function

    def submit_calls(self, rs, resources=None, resources_mode=None, environment=None):
        resources_already_set = set()
        for r in rs:
            k, (fn, *args) = r
            t = PythonTaskDask(k, fn, *args)

            cat = str(fn)
            t.set_category(cat)

            if cat not in resources_already_set:
                if resources_mode:
                    self.set_category_mode(cat, resources_mode)
                    self.set_category_resources_max(cat, resources)
                    self.enable_monitoring()

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

        d = DaskVineDag(dsk)
        rs = d.set_targets(keys_flatten)

        verbose = kwargs.pop("verbose", False)

        self.submit_calls(rs, **kwargs)

        while not self.empty():
            t = self.wait(5)
            if t:
                if t.successful():
                    if verbose:
                        print(f"{t.key} ran on {t.hostname} with result {t.output}")
                    rs = d.set_result(t.key, t.output)
                    self.submit_calls(rs, **kwargs)
                else:
                    raise Exception(f"task for key {t.key} failed: {t.result}. exit code {t.exit_code}\n{t.output}")

        if isinstance(keys, list):
            results = list(keys)
            for k, ids in indices.items():
                DaskVineDag.set_dask_result(results, ids, d.get_result(k))
        else:
            results = d.get_result(keys)
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
    def __init__(self, key, fn, *args, **kwargs):
        self._key = key
        super().__init__(fn, *args, **kwargs)

    @property
    def key(self):
        return self._key
