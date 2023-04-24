##
# @package ndcctools.taskvine.dask_executor
#
# This module provides a specialized manager @ref ndcctools.taskvine.dask_executor.DaskVine to execute
# dask workflows.

# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import ndcctools.taskvine as vine
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
# Parameters for execution can be set as arguments to the compute function. For
# example, to set resources per dask function call:
#
# @code
# with dask.config.set(scheduler=m.dask_execute):
#     result = v.compute(resources={"cores": 1})
# @endcode


class DaskVine(vine.Manager):
    def _wrap(self, key, fn):
        # TODO: return backtrace, write result to files, read args from files
        def function(*args):
            try:
                result = fn(args)
            except Exception as e:
                result = e
            return (key, result)
        return function

    def submit_calls(self, rs, resources=None):
        for r in rs:
            k, (fn, *args) = r
            t = PythonTaskDask(k, fn, *args)

            if resources:
                t.set_cores(resources.get("cores", -1))
            self.submit(t)

    def dask_execute(self, dsk, keys, **kwargs):
        """Computes the values of the keys in the dask graph dsk"""

        indices = DaskVineDag.find_dask_keys(keys)
        d = DaskVineDag(dsk)
        rs = d.set_targets(indices.keys())
        self.submit_calls(rs, **kwargs)

        verbose = kwargs.get("verbose", False)

        while not self.empty():
            t = self.wait(5)
            if t:
                if t.successful():
                    if verbose:
                        print(f"{t.key} ran on {t.hostname} with result {t.output}")
                    rs = d.set_result(t.key, t.output)
                    self.submit_calls(rs, **kwargs)
                else:
                    raise Exception(f"task for key {t.key} failed: {t.result}. exit code {t.exit_code}")

        results = list(keys)
        for k, ids in indices.items():
            DaskVineDag.set_dask_result(results, ids, d.get_result(k))
        return results


class PythonTaskDask(vine.PythonTask):
    def __init__(self, key, fn, *args, **kwargs):
        self._key = key
        super().__init__(fn, *args, **kwargs)

    @property
    def key(self):
        return self._key
