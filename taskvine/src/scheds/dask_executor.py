import taskvine as vine
import sys
from uuid import uuid4
from collections import defaultdict

from operator import add


class Dag:
    """A directed graph that encodes the steps and state a computation needs.
    Single computations are encoded as s-expressions, therefore it is 'upside-down',
    in the sense that the children of a node are the nodes required to compute it.
    E.g., for

    dsk = {'x': 1,
           'y': 2,
           'z': (add, 'x', 'y'),
           'w': (sum, ['x', 'y', 'z']),
           'v': [(sum, ['w', 'z']), 2]
           }

    'z' has as children 'x' and 'y'.

    Each node is referenced by its key. When the value of a key is list of
    sexprs, like 'v' above, then a key is automatically computed recursively
    for each computation.

    Computation is done lazily. The Dag is initialized from a task graph, but not
    computation is decoded. To use the Dag:
        - Dag.add(key): Request the computation associated with key to be decoded.
        - Dag.get_ready(): A list of (key, fn, *args) of functions that are ready
          to be executed.
        - Dag.set_computing(key): Marks key as currently being computed.
          (I.e., it doesn't have a result, but does not appear in Dag.ready())
        - Dag.get_computing(): A list of keys being computed.
        - Dag.set_result(key, value): Sets the result of key to value.
        - Dag.get_result(key): Get result associated with key. Raises DagNoResult
          if key does not have a result yet."""

    @staticmethod
    def symbolp(s):
        return not isinstance(s, (tuple, list))

    @staticmethod
    def fun_callp(s):
        if not isinstance(s, tuple):
            return False
        if len(s) < 1:
            return False
        return callable(s[0])

    @staticmethod
    def find_dask_keys(item, indices=None):
        if not indices:
            indices = []

        keys = {}
        if isinstance(item, list):
            indices.append(0)
            for s in item:
                keys.update(Dag.find_dask_keys(s, indices))
        else:
            last = indices[-1]
            keys[item] = list(indices)
            indices[-1] = last + 1
        return keys

    @staticmethod
    def set_dask_result(lst, indices, value):
        inner = lst
        for i in indices[:-1]:
            inner = inner[i]
        inner[indices[-1]] = value

    def __init__(self, dsk):
        self._dsk = dsk

        # those sexpr from dsk that we need to compute, but flatten
        self._flat = {}

        # child -> parents. I.e., which parents needs the result of child
        self._parents_of = defaultdict(lambda: set())

        # parent->children. I.e., the dependencies of parent
        self._children_of = defaultdict(lambda: set())

        # parent->children still waiting for result. A key is ready to be computed when children left is []
        self._missing_of = {}

        # key->value of its computation
        self._result_of = {}

        # target keys that the dag should compute
        self._targets = set()

        # set of keys which functions are ready to be computed
        # only tasks of the form (fn, arg1, arg2, ...) are ever added to the ready set.
        self._ready = set()

        # set of keys currently being computed.
        self._computing = set()

    def graph_keyp(self, s):
        return Dag.symbolp(s) and s in self._dsk

    def flatten(self, key):
        """ Recursively decomposes a sexpr associated with key, so that its arguments, if any
        are keys. """
        sexpr = self._dsk[key]
        self.flatten_rec(key, sexpr)

    def flatten_rec(self, key, sexpr):
        def relate(parent, child):
            self._parents_of[child].add(parent)
            self._children_of[parent].add(child)
            if not self.has_result(child):
                self._missing_of[parent].add(child)

        if key in self._flat:
            # this key has already been considered
            return

        if Dag.symbolp(sexpr):
            self._flat[key] = sexpr
            self._result_of[key] = sexpr
        else:
            self._missing_of[key] = set()
            nargs = []
            if Dag.fun_callp(sexpr):
                # if this is a function call, keep the function as is
                nargs.append(sexpr[0])
                sexpr = sexpr[1:]
            for a in sexpr:
                if Dag.symbolp(a) and not self.graph_keyp(a):
                    nkey = a
                else:
                    if self.graph_keyp(a):
                        nkey = a
                        self.flatten_rec(nkey, self._dsk[nkey])
                    else:
                        nkey = uuid4()
                        self.flatten_rec(nkey, a)
                    relate(parent=key, child=nkey)
                nargs.append(nkey)

            self._flat[key] = type(sexpr)(nargs)  # reconstruct from generated keys

    def has_result(self, key):
        return key in self._result_of

    def get_result(self, key):
        """ Sets new result and propagates in the Dag. Returns a list of [key, (fn, *args)]
        of computations that become ready to be executed """
        try:
            return self._result_of[key]
        except KeyError:
            raise DagNoResult(key)

    def fill_seq(self, sexpr):
        lst = []
        for c in sexpr:
            if c in self._flat:
                lst.append(self.get_result(c))
            else:
                lst.append(c)
        return type(sexpr)(lst)

    def fill_call(self, sexpr):
        lst = (sexpr[0], *self.fill_seq(sexpr[1:]))
        return lst

    def set_result(self, key, value):
        """ Sets new result and propagates in the Dag. Returns a list of [key, (fn, *args)]
        of computations that become ready to be executed """
        new_ready = []
        self._result_of[key] = value
        for p in self._parents_of[key]:
            self._missing_of[p].discard(key)
            if self._missing_of[p] or self.has_result(p):
                continue
            sexpr = self._flat[p]
            if Dag.fun_callp(sexpr):
                new_ready.append([p, self.fill_call(sexpr)])
            else:
                new_ready.extend(self.set_result(p, self.fill_seq(sexpr)))
        return new_ready

    def get_ready(self):
        """ List of [key, (fn, *args)] ready for computation.
        This is a potentially expensive call and should be used only for
        bootstrapping. Further calls should use Dag.set_result to discover
        the new computations that become ready to be executed. """
        rs = []
        for (p, cs) in self._missing_of.items():
            if self.has_result(p):
                continue
            if cs:
                continue
            sexpr = self._flat[p]
            if Dag.fun_callp(sexpr):
                rs.append([p, self.fill_call(sexpr)])
            else:
                rs.extend(self.set_result(p, self.fill_seq(sexpr)))
        return rs

    def set_targets(self, keys):
        """ Values of keys that need to be computed. """
        self._targets.update(keys)

        for k in keys:
            if k in self._flat:
                # this key has already been considered
                continue
            self.flatten(k)
        return self.get_ready()


class DagNoResult(Exception):
    """Exception raised when asking for a result from a computation that has not been performed."""

    pass


class DaskVine(vine.Manager):
    def _wrap(self, key, fn):
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

            m.submit(t)

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
                    raise Exception(f"task for key {t.key} failed: {t.result_string}")

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

    return t

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


            t = simple_delayed_graph()
            print(t.compute())
    sys.exit(0)
