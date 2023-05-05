# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from uuid import uuid4
from collections import defaultdict


class DaskVineDag:
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

    Computation is done lazily. The DaskVineDag is initialized from a task graph, but not
    computation is decoded. To use the DaskVineDag:
        - DaskVineDag.set_targets(keys): Request the computation associated with key to be decoded.
        - DaskVineDag.get_ready(): A list of [key, (fn, *args)] of functions that are ready
          to be executed.
        - DaskVineDag.set_result(key, value): Sets the result of key to value.
        - DaskVineDag.get_result(key): Get result associated with key. Raises DagNoResult
        - DaskVineDag.has_result(key): Whether the key has a computed result. """

    @staticmethod
    def keyp(s):
        return DaskVineDag.hashable(s) and not DaskVineDag.taskp(s)

    @staticmethod
    def taskp(s):
        return isinstance(s, tuple) and len(s) > 0 and callable(s[0])

    @staticmethod
    def listp(s):
        return isinstance(s, list)

    @staticmethod
    def symbolp(s):
        return not (DaskVineDag.keyp(s) or DaskVineDag.taskp(s) or DaskVineDag.listp(s))

    @staticmethod
    def hashable(s):
        try:
            hash(s)
            return True
        except TypeError:
            return False

    @staticmethod
    def find_dask_keys(item, indices=None):
        if not indices:
            indices = []

        keys = {}
        if isinstance(item, list):
            indices.append(0)
            for s in item:
                keys.update(DaskVineDag.find_dask_keys(s, indices))
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
        self._dsk = dict(dsk)

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
        if DaskVineDag.keyp(s):
            return s in self._dsk
        return False

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

        self._missing_of[key] = set()

        if self.graph_keyp(sexpr):
            self._flat[key] = sexpr
            relate(parent=key, child=sexpr)
        elif not (DaskVineDag.taskp(sexpr) or DaskVineDag.listp(sexpr)):
            self._flat[key] = sexpr
            self._result_of[key] = sexpr
        else:
            nargs = []
            next_flat = []

            if DaskVineDag.listp(sexpr):
                sexpr = (make_list, *sexpr)

            nargs.append(sexpr[0])
            sexpr = sexpr[1:]

            for arg in sexpr:
                if self.graph_keyp(arg):
                    nargs.append(arg)
                    next_flat.append((arg, self._dsk[arg]))
                elif DaskVineDag.symbolp(arg):
                    nargs.append(arg)
                else:
                    next_key = uuid4()
                    nargs.append(next_key)
                    next_flat.append((next_key, arg))
            self._flat[key] = tuple(nargs)  # reconstruct from generated keys
            for (n, a) in next_flat:
                self.flatten_rec(n, a)
                relate(parent=key, child=n)

    def has_result(self, key):
        return key in self._result_of

    def get_result(self, key):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of [key, (fn, *args)]
        of computations that become ready to be executed """
        try:
            return self._result_of[key]
        except KeyError:
            raise DaskVineNoResult(key)

    def fill_seq(self, sexpr):
        lst = []
        for c in sexpr:
            if DaskVineDag.keyp(c) and c in self._flat:
                lst.append(self.get_result(c))
            else:
                lst.append(c)
        return type(sexpr)(lst)

    def fill_call(self, sexpr):
        lst = (sexpr[0], *self.fill_seq(sexpr[1:]))
        return lst

    def set_result(self, key, value):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of [key, (fn, *args)]
        of computations that become ready to be executed """
        new_ready = []
        self._result_of[key] = value
        for p in self._parents_of[key]:
            self._missing_of[p].discard(key)
            if self._missing_of[p] or self.has_result(p):
                continue
            sexpr = self._flat[p]
            if DaskVineDag.taskp(sexpr):
                new_ready.append([p, self.fill_call(sexpr)])
            else:
                new_ready.extend(self.set_result(p, self.fill_seq(sexpr)))
        return new_ready

    def get_ready(self):
        """ List of [key, (fn, *args)] ready for computation.
        This is a potentially expensive call and should be used only for
        bootstrapping. Further calls should use DaskVineDag.set_result to discover
        the new computations that become ready to be executed. """
        rs = []
        for (p, cs) in self._missing_of.items():
            if self.has_result(p):
                continue
            if cs:
                continue
            sexpr = self._flat[p]
            if DaskVineDag.taskp(sexpr):
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

    def get_targets(self):
        return self._targets


class DaskVineNoResult(Exception):
    """Exception raised when asking for a result from a computation that has not been performed."""
    pass


# aux function to make lists from many arguments, a little silly, but list()
# wants only one argument and list(sometuple) does not work because we want to
# flatten the tuple.
def make_list(*args):
    return args
