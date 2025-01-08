# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from collections import defaultdict
import dask._task_spec as dts


class DaskVineDag:
    """A directed graph that encodes the steps and state a computation needs.
    Single computations are encoded as dts.Task's, with dependecies expressed as the keys needed by the task.

    dsk = {'x': 1,
           'y': 2,
           'z': dts.Task('z', add, dts.TaskRef('x'), dts.TaskRef('y'))
           'w': dts.Task('w', sum, [dts.TaskRef('x'), dts.TaskRef('y'), dts.TaskRef('z')]),
           'v': dts.Task('v', sum, [dts.TaskRef('w'), dts.TaskRef('z')])
           't': dts.Task('v', sum, [dts.TaskRef('v'), 2])
           }

    'z' has as dependecies 'x' and 'y'.

    Computation is done lazily. The DaskVineDag is initialized from a task graph, but not
    computation is decoded. To use the DaskVineDag:
        - DaskVineDag.set_targets(keys): Request the computation associated with key to be decoded.
        - DaskVineDag.get_ready(): A list of dts.Task that are ready to be executed.
        - DaskVineDag.set_result(key, value): Sets the result of key to value.
        - DaskVineDag.get_result(key): Get result associated with key. Raises DagNoResult
        - DaskVineDag.has_result(key): Whether the key has a computed result. """

    @staticmethod
    def hashable(s):
        try:
            hash(s)
            return True
        except TypeError:
            return False

    @staticmethod
    def keyp(s):
        return DaskVineDag.hashable(s) and not DaskVineDag.taskref(s) and not DaskVineDag.taskp(s)

    @staticmethod
    def taskref(s):
        return isinstance(s, (dts.TaskRef, dts.Alias))

    @staticmethod
    def taskp(s):
        return isinstance(s, dts.Task)

    @staticmethod
    def listlikep(s):
        return isinstance(s, (list, tuple))

    @staticmethod
    def symbolp(s):
        return isinstance(s, dts.DataNode)

    def __init__(self, dsk):
        self._dsk = dsk

        #  For a key, the set of keys that need it to perform a computation.
        self._needed_by = defaultdict(lambda: set())

        # For a key, the subset of self._needed_by[key] that still need to be completed.
        # Only useful for gc.
        self._pending_needed_by = defaultdict(lambda: set())

        # For a key, the set of keys that it needs for computation.
        self._dependencies_of = {}

        # For a key, the set of keys with a pending result for they key to be computed.
        # When the set is empty, the key is ready to be computed. It is always a subset
        # of self._dependencies_of[key].
        self._missing_of = {}

        # key->value of its computation
        self._result_of = {}

        # key->depth. The shallowest level the key is found
        self._depth_of = defaultdict(lambda: float('inf'))

        # target keys that the dag should compute
        self._targets = set()

        self._working_graph = dict(dsk)

        self.initialize_graph()

    def left_to_compute(self):
        return len(self._working_graph) - len(self._result_of)

    def depth_of(self, key):
        return self._depth_of[key]

    def initialize_graph(self):
        for task in self._working_graph.values():
            self.set_relations(task)

        for task in self._working_graph.values():
            if isinstance(task, dts.DataNode):
                self._depth_of[task.key] = 0
                self.set_result(task.key, task.value)

    def set_relations(self, task):
        self._dependencies_of[task.key] = task.dependencies
        self._missing_of[task.key] = set(self._dependencies_of[task.key])
        for c in self._dependencies_of[task.key]:
            self._needed_by[c].add(task.key)
            self._pending_needed_by[c].add(task.key)

    def get_ready(self):
        """ List of dts.Task ready for computation.
        This call should be used only for
        bootstrapping. Further calls should use DaskVineDag.set_result to discover
        the new computations that become ready to be executed. """
        rs = {}
        for (key, cs) in self._missing_of.items():
            if self.has_result(key) or cs:
                continue
            node = self._working_graph[key]
            if self.taskref(node):
                rs.update(self.set_result(key, self.get_result(node.key)))
            elif self.symbolp(node):
                rs.update(self.set_result(key, node))
            else:
                rs[key] = node

        for r in rs:
            if self._dependencies_of[r]:
                self._depth_of[r] = min(self._depth_of[d] for d in self._dependencies_of[r]) + 1
            else:
                self._depth_of[r] = 0

        return rs.values()

    def set_result(self, key, value):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of dts.Task
        of computations that become ready to be executed """
        rs = {}
        self._result_of[key] = value
        for p in self._pending_needed_by[key]:
            self._missing_of[p].discard(key)

            if self._missing_of[p]:
                # the key p still has dependencies unmet...
                continue

            node = self._working_graph[p]
            if self.taskref(node):
                rs.update(
                    self.set_result(p, self.get_result(node))
                )  # case e.g, "x": "y", and we just set the value of "y"
            elif self.symbolp(node):
                rs.update(self.set_result(p, node))
            else:
                rs[p] = node

        for r in rs:
            if self._dependencies_of[r]:
                self._depth_of[r] = min(self._depth_of[d] for d in self._dependencies_of[r]) + 1
            else:
                self._depth_of[r] = 0

        for c in self._dependencies_of[key]:
            self._pending_needed_by[c].discard(key)

        return rs.values()

    def _add_second_targets(self, key):
        v = self._working_graph[key]
        if self.taskref(v):
            lst = [v]
        elif DaskVineDag.listlikep(v):
            lst = v
        else:
            return
        for c in lst:
            if self.taskref(c):
                self._targets.add(c.key)
                self._add_second_targets(c.key)

    def has_result(self, key):
        return key in self._result_of

    def get_result(self, key):
        try:
            return self._result_of[key]
        except KeyError:
            raise DaskVineNoResult(key)

    def get_dependencies(self, key):
        return self._dependencies_of[key]

    def get_missing_dependencies(self, key):
        return self._missing_of[key]

    def get_needed_by(self, key):
        return self._needed_by[key]

    def get_pending_needed_by(self, key):
        return self._pending_needed_by[key]

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
