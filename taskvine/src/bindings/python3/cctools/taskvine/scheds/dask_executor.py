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

    def dask_get(self, dsk, keys, **kwargs):
        """Computes the values of the keys in the dask graph dsk"""

        indices = Dag.find_dask_keys(keys)
        d = Dag(dsk)
        rs = d.set_targets(indices.keys())
        self.submit_calls(rs, **kwargs)

        while not self.empty():
            t = m.wait(5)
            if t:
                if t.successful():
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


if __name__ == '__main__':
    def test_graph():
        dsk = {
            "x": 1,
            "y": 2,
            "z": (add, "x", "y"),
            "w": (sum, ["x", "y", "z"]),
            "v": [(sum, ["w", "z"]), 2],
            "t": (sum, "v")
        }

        d = Dag(dsk)
        rs = d.set_targets(["t"])
        while rs:
            k, (fn, *args) = rs.pop()
            val = fn(*args)
            rs.extend(d.set_result(k, val))

        final_value = d.get_result("t")
        expected = 11

        print(f"value {final_value}. expected {expected}")
        return final_value == expected


    def simple_delayed_graph():
        import dask.delayed

        @dask.delayed(pure=True)
        def sum_d(args):
            return sum(args)

        @dask.delayed(pure=True)
        def add_d(*args):
            return add(*args)

        @dask.delayed(pure=True)
        def list_d(*args):
            return list(args)

        z = add_d(1, 2)
        w = sum_d([1, 2, z])
        v = [sum_d([z, w]), 2]
        t = sum_d(v)

        m = DaskVine(port=0, ssl=True)

        f = vine.Factory(manager=m)
        f.cores = 4
        f.max_workers = 1
        f.min_workers = 1
        with f:
            with dask.config.set(scheduler=m.dask_get):
                t = simple_delayed_graph()
                print(t.compute())

        try:
            import awkward as ak
            import dask
            import dask_awkward as dak
            import numpy as np

            behavior: dict = {}

            @ak.mixin_class(behavior)
            class Point:
                def distance(self, other):
                    return np.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

            if __name__ == "__main__":
                points1 = ak.Array([
                    [{"x": 1.0, "y": 1.1}, {"x": 2.0, "y": 2.2}, {"x": 3, "y": 3.3}],
                    [],
                    [{"x": 4.0, "y": 4.4}, {"x": 5.0, "y": 5.5}],
                    [{"x": 6.0, "y": 6.6}],
                    [{"x": 7.0, "y": 7.7}, {"x": 8.0, "y": 8.8}, {"x": 9, "y": 9.9}],
                ])

                points2 = ak.Array([
                    [{"x": 0.9, "y": 1.0}, {"x": 2.0, "y": 2.2}, {"x": 2.9, "y": 3.0}],
                    [],
                    [{"x": 3.9, "y": 4.0}, {"x": 5.0, "y": 5.5}],
                    [{"x": 5.9, "y": 6.0}],
                    [{"x": 6.9, "y": 7.0}, {"x": 8.0, "y": 8.8}, {"x": 8.9, "y": 9.0}],
                ])

                array1 = dak.from_awkward(points1, npartitions=3)
                array2 = dak.from_awkward(points2, npartitions=3)

                array1 = dak.with_name(array1, name="Point", behavior=behavior)
                array2 = dak.with_name(array2, name="Point", behavior=behavior)

                distance = array1.distance(array2)

                m = DaskVine(port=0, ssl=True)

                f = vine.Factory(manager=m)
                f.cores = 4
                f.max_workers = 1
                f.min_workers = 1
                with f:
                    with dask.config.set(scheduler=m.dask_get):
                        result = distance.compute(resources={"cores": 1})
                        print(result)
        except ImportError as e:
            print(f"module not available for example: {e}")
