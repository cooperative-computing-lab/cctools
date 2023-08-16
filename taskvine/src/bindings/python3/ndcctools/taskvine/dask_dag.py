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
    sexprs, like 'v' above, and low_memory_mode is True, then a key is automatically computed recursively
    for each computation.

    Computation is done lazily. The DaskVineDag is initialized from a task graph, but not
    computation is decoded. To use the DaskVineDag:
        - DaskVineDag.set_targets(keys): Request the computation associated with key to be decoded.
        - DaskVineDag.get_ready(): A list of [key, sexpr] of expressions that are ready
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
        return not (DaskVineDag.taskp(s) or DaskVineDag.listp(s))

    @staticmethod
    def hashable(s):
        try:
            hash(s)
            return True
        except TypeError:
            return False

    def __init__(self, dsk, low_memory_mode=False):
        self._dsk = dsk

        # child -> parents. I.e., which parents needs the result of child
        self._parents_of = defaultdict(lambda: set())

        # parent->children still waiting for result. A key is ready to be computed when children left is []
        self._missing_of = {}

        # parent->nchildren get the number of children for parent computation
        self._children_of = {}

        # key->value of its computation
        self._result_of = {}

        # key->depth. The shallowest level the key is found
        self._depth_of = defaultdict(lambda: float('inf'))

        # target keys that the dag should compute
        self._targets = set()

        # set of keys which functions are ready to be computed
        # only tasks of the form (fn, arg1, arg2, ...) are ever added to the ready set.
        self._ready = set()

        # set of keys currently being computed.
        self._computing = set()

        self._working_graph = dict(dsk)
        if low_memory_mode:
            self._flatten_graph()

        self.initialize_graph()

    def graph_keyp(self, s):
        if DaskVineDag.keyp(s):
            return s in self._working_graph
        return False

    def depth_of(self, key):
        return self._depth_of[key]

    def initialize_graph(self):
        for key, sexpr in self._working_graph.items():
            self.set_relations(key, sexpr)

    def find_dependencies(self, sexpr, depth=0):
        dependencies = set()
        if self.graph_keyp(sexpr):
            dependencies.add(sexpr)
            self._depth_of[sexpr] = min(depth, self._depth_of[sexpr])
        elif not DaskVineDag.symbolp(sexpr):
            for sub in sexpr:
                dependencies.update(self.find_dependencies(sub, depth + 1))
        return dependencies

    def set_relations(self, key, sexpr):
        sexpr = self._working_graph[key]
        self._children_of[key] = self.find_dependencies(sexpr)
        self._missing_of[key] = set(self._children_of[key])

        for c in self._children_of[key]:
            self._parents_of[c].add(key)

    def get_ready(self):
        """ List of [(key, sexpr),...] ready for computation.
        This call should be used only for
        bootstrapping. Further calls should use DaskVineDag.set_result to discover
        the new computations that become ready to be executed. """
        rs = []
        for (key, cs) in self._missing_of.items():
            if self.has_result(key) or cs:
                continue
            sexpr = self._working_graph[key]
            if self.graph_keyp(sexpr):
                rs.extend(self.set_result(key, self.get_result(sexpr)))
            elif self.symbolp(sexpr):
                rs.extend(self.set_result(key, sexpr))
            else:
                rs.append((key, sexpr))
        return rs

    def set_result(self, key, value):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of [(key, sexpr),...]
        of computations that become ready to be executed """

        rs = []
        self._result_of[key] = value
        for p in self._parents_of[key]:
            self._missing_of[p].discard(key)

            if self._missing_of[p]:
                continue

            sexpr = self._working_graph[p]
            if self.graph_keyp(sexpr):
                rs.extend(
                    self.set_result(p, self.get_result(sexpr))
                )  # case e.g, "x": "y", and we just set the value of "y"
            elif self.symbolp(sexpr):
                rs.extend(self.set_result(p, sexpr))
            else:
                rs.append((p, sexpr))
        return rs

    def _flatten_graph(self):
        """ Recursively decomposes a sexpr associated with key, so that its arguments, if any
        are keys. """
        for key in list(self._working_graph.keys()):
            self.flatten_rec(key, self._working_graph[key], toplevel=True)

    def _add_second_targets(self, key):
        if not DaskVineDag.listp(self._working_graph[key]):
            return
        for c in self._working_graph[key]:
            if self.graph_keyp(c):
                self._targets.add(c)
                self._add_second_targets(c)

    def flatten_rec(self, key, sexpr, toplevel=False):
        if key in self._working_graph and not toplevel:
            return
        if DaskVineDag.symbolp(sexpr):
            return

        nargs = []
        next_flat = []
        cons = type(sexpr)

        for arg in sexpr:
            print(arg)
            if DaskVineDag.symbolp(arg):
                nargs.append(arg)
            else:
                next_key = uuid4()
                nargs.append(next_key)
                next_flat.append((next_key, arg))

        self._working_graph[key] = cons(nargs)
        for (n, a) in next_flat:
            self.flatten_rec(n, a)

    def has_result(self, key):
        return key in self._result_of

    def get_result(self, key):
        try:
            return self._result_of[key]
        except KeyError:
            raise DaskVineNoResult(key)

    def get_children(self, key):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of [key, (fn, *args)]
        of computations that become ready to be executed """
        try:
            return self._children_of[key]
        except KeyError:
            raise DaskVineNoResult(key)

    def set_targets(self, keys):
        """ Values of keys that need to be computed. """
        self._targets.update(keys)
        for k in keys:
            self._add_second_targets(k)
        return self.get_ready()

    def get_targets(self):
        return self._targets


class DaskVineNoResult(Exception):
    """Exception raised when asking for a result from a computation that has not been performed."""
    pass
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
