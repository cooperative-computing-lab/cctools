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

    def __init__(self, dsk):
        self._dsk = dsk

        # those sexpr from dsk that we need to compute, but flatten
        self._flat = {}

        # child -> parent. I.e., which parent needs the result of child
        self._parents = {}

        # parent->children. I.e., the dependencies of parent
        self._children = {}

        # parent->children still waiting for result. A key is ready to be computed when children left is []
        self._children_left = defaultdict(lambda: [])

        # key->value of its computation
        self._results = {}

        # set of keys which functions are ready to be computed
        # only tasks of the form (fn, arg1, arg2, ...) are ever added to the ready set.
        self._ready = set()

        # set of keys currently being computed.
        self._computing = set()

    def flatten(self, key):
        """ Recursively decomposes a sexpr associated with key, so that its arguments, if any
        are keys. Returns two dictionaries: a subgraph corresponding to the sexpr, and the
        children of the keys that have them."""
        sexpr = self._dsk[key]
        return self.flatten_rec(key, sexpr)

    def flatten_rec(self, key, sexpr):
        def symbolp(s):
            return not isinstance(s, (tuple, list))

        def graph_key(s):
            return symbolp(s) and s in self._dsk

        subgraph = {}
        parents = {}
        children = defaultdict(lambda: [])

        if symbolp(sexpr):
            subgraph[key] = sexpr
        else:
            nargs = []
            cons = type(sexpr)

            if isinstance(sexpr, tuple):
                sexpr = list(sexpr)
                nargs.append(sexpr.pop(0))

            for a in sexpr:
                if symbolp(a) and not graph_key(a):
                    nargs.append(a)
                    continue

                if graph_key(a):
                    nkey = a
                    (ns, nc, np) = self.flatten(nkey)
                else:
                    nkey = uuid4()
                    (ns, nc, np) = self.flatten_rec(nkey, a)
                nargs.append(nkey)
                children[key].append(nkey)
                parents[nkey] = key

                subgraph.update(ns)
                children.update(nc)
                parents.update(np)

            subgraph[key] = cons(nargs)
        return (subgraph, children, parents)

    def set_targets(self, *keys):
        """ Values of keys that need to be computed. """
        keys = set(keys)  # remove possible duplicates
        for k in keys:
            if k in self._flat:
                # this key has already been considered
                continue
            (subgraph, children, parents) = self.flatten(k)
            self._flat.update(subgraph)
            self._children.update(children)
            self._parents.update(parents)

            for (p, cs) in children.items():
                for c in cs:
                    if c not in self._results:
                        self._children_left[p].append(c)
            print(self._flat, self._children, self._children_left, self._parents)



    def ready(self):
        rs = []
        for key in self._ready:
            task = self._task
            # return [(key, )]

    @property
    def key(self):
        pass

    def parent(self):
        """Which key"""
        pass


class Task:
    def __init__(self, key, *args):
        self._done = False
        self._result = None
        self._sexpr = None

    def done(self):
        return self._done

    def result(self):
        if self.done():
            return self._result
        else:
            raise DagNoResult

    def set_result(self, value):
        self._result = value


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

    def dask_get(self, dsk, keys, **kwargs):
        """Computes the values of the keys in the dask graph dsk"""
        results = {}
        needed = list(keys)
        waiting = set()
        dependants = defaultdict(lambda: set)
        missing = defaultdict(lambda: set)
        ready = set()

        while needed or waiting_queue:
            head = needed.pop(0)
            if head in results:
                continue
            waiting_keys.add(head)

            task = dsk[head]
            if isinstance(task, list):
                pass
            if isinstance(task, tuple):
                pass
            else:
                result[head] = task


if __name__ == "__main__":
    dsk = {
        "x": 1,
        "y": 2,
        "z": (add, "x", "y"),
        "w": (sum, ["x", "y", "z"]),
        "v": [(sum, ["w", "z"]), 2],
    }

    d = Dag(dsk)
    d.set_targets("v")

    sys.exit(0)

    m = DaskVine(port=0, ssl=True)

    f = vine.Factory(manager=m)
    f.cores = 4
    f.max_workers = 1
    f.min_workers = 1

    with f:
        print(m.dask_get(dsk, "w"))
