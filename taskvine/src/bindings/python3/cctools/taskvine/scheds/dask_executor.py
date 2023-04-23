import cctools.taskvine as vine

import sys
from cctools.taskvine.scheds.dag import Dag, DagNoResult

from operator import add


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

        indices = Dag.find_dask_keys(keys)
        d = Dag(dsk)
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
            Dag.set_dask_result(results, ids, d.get_result(k))
        return results


class PythonTaskDask(vine.PythonTask):
    def __init__(self, key, fn, *args, **kwargs):
        self._key = key
        super().__init__(fn, *args, **kwargs)

    @property
    def key(self):
        return self._key
