# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from collections import defaultdict
import dask._task_spec as dts
import math


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
    def containerp(s):
        return isinstance(s, dts.NestedContainer)

    @staticmethod
    def symbolp(s):
        return isinstance(s, dts.DataNode)

    def __init__(self, dsk, reconstruct=False, merge_size=2):
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
        self._target_map = {}

        self._working_graph = dict(dsk)

        if reconstruct:
            self.expand_merge(merge_size)

        self.initialize_graph()

    def expand_merge(self, merge_size):
        graph_copy = self._working_graph.copy()
        to_remove = []
        for key, sexpr in graph_copy.items():
            if isinstance(sexpr, dts.Task):
                if callable(sexpr.func) and hasattr(sexpr.func, '__name__') and sexpr.func.__name__ == "flex_merge":
                    new_key = self.rebuild_merge(key, sexpr, merge_size)
                    to_remove.append(key)
                    self._target_map[key] = new_key
        for key, sexpr in graph_copy.items():
            if isinstance(sexpr, dts.Task):
                for d in sexpr.dependencies:
                    if d in self._target_map:
                        new_deps = frozenset([self._target_map[d] if d in self._target_map else d for d in sexpr.dependencies])
                        new_args = []
                        for arg in sexpr.args:
                            if not isinstance(arg, str) and arg.key in self._target_map:
                                new_args.append(dts.Alias(self._target_map[arg.key]))
                            else:
                                new_args.append(arg)
                        self._working_graph[key].args = new_args
                        self._working_graph[key]._dependencies = new_deps
        for key in to_remove:
            del self._working_graph[key]

    def rebuild_merge(self, key, sexpr, merge_size):
        chunk_size = merge_size
        new_sexprs = []

        base_key = sexpr.key
        jumps = math.ceil(len(sexpr.args) / chunk_size)
        count = 0
        for i in range(jumps):
            count += 1
            print(count)
            sexpr_copy = sexpr.copy()
            new_args = sexpr.args[i * chunk_size:i * chunk_size + chunk_size]
            new_dependencies = set()
            for arg in new_args:
                new_dependencies.add(arg.key)
            new_dependencies = frozenset(new_dependencies)
            new_key = f'{base_key}-{count}'

            sexpr_copy.key = new_key
            sexpr_copy.args = new_args
            sexpr_copy._dependencies = new_dependencies
            new_sexprs.append(sexpr_copy)
            self._working_graph[new_key] = sexpr_copy

        while (len(new_sexprs) > 1):
            print(count)
            count += 1
            sexpr_copy = sexpr.copy()
            new_args = [dts.Alias(new_sexprs.pop(0).key) for i in range(chunk_size) if new_sexprs]
            new_dependencies = set()
            for arg in new_args:
                new_dependencies.add(arg.key)
            new_dependencies = frozenset(new_dependencies)
            new_key = f'{base_key}-{count}'

            sexpr_copy.key = new_key
            sexpr_copy.args = new_args
            sexpr_copy._dependencies = new_dependencies
            new_sexprs.append(sexpr_copy)
            self._working_graph[new_key] = sexpr_copy

        return new_sexprs[0].key

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
            if DaskVineDag.taskref(node):
                rs.update(self.set_result(key, self.get_result(node.key)))
            elif DaskVineDag.symbolp(node):
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
        if 'merge' in key:
            print(key)
        self._result_of[key] = value
        for p in self._pending_needed_by[key]:
            self._missing_of[p].discard(key)

            if self._missing_of[p]:
                # the key p still has dependencies unmet...
                continue
            if p in self._target_map:
                continue
            node = self._working_graph[p]
            if DaskVineDag.taskref(node):
                x = self.set_result(p, self.get_result(node.target))
                # rs.update(x)  # case e.g, "x": "y", and we just set the value of "y"
                rs = x  # case e.g, "x": "y", and we just set the value of "y"
            elif DaskVineDag.symbolp(node):
                rs.update(self.set_result(p, node))
            else:
                rs[p] = node

        for r in rs:
            if self._dependencies_of[r]:
                self._depth_of[r] = min(self._depth_of[d] for d in self._dependencies_of[r]) + 1
            else:
                self._depth_of[r] = 0

        if not DaskVineDag.taskref(self._working_graph[key]):
            for c in self._dependencies_of[key]:
                self._pending_needed_by[c].discard(key)

        if DaskVineDag.taskref(self._working_graph[key]):
            return rs
        else:
            return rs.values()

    def _add_second_targets(self, key):
        v = self._working_graph[key]
        if DaskVineDag.taskref(v):
            lst = [v]
        elif DaskVineDag.containerp(v):
            lst = v
        else:
            return
        for c in lst:
            if DaskVineDag.taskref(c):
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

    def replace_targets(self, keys):
        return [self._target_map[key] if key in self._target_map else key for key in keys]

    def set_targets(self, keys):
        """ Values of keys that need to be computed. """
        self._targets.update(keys)
        for k in keys:
            self._add_second_targets(k)
        return self.get_ready()

    def get_targets(self):
        return self._targets

    def visualize(self, name='DaskVine_DAG'):
        import graphviz
        nodes = []
        dot = graphviz.Digraph(name)
        for item in self._working_graph:
            sexpr = self._working_graph[item]
            if isinstance(sexpr, dts.Task):
                if str(item) not in nodes:
                    dot.node(str(item), str(item))
                    nodes.append(str(item))
                for value in sexpr.dependencies:
                    if value in self._working_graph:
                        if isinstance(self._working_graph[value], dts.Task):
                            if str(value) not in nodes:
                                dot.node(str(value), str(value))
                                nodes.append(str(value))
                            dot.edge(str(value), str(item))
                        elif isinstance(self._working_graph[value], dts.Alias):
                            value = self._working_graph[value].target
                            if str(value) not in nodes:
                                dot.node(str(value), str(value))
                                nodes.append(str(value))
                            dot.edge(str(value), str(item))
                        else:
                            pass
        dot.render(f'doctest-output/{name}')


class DaskVineNoResult(Exception):
    """Exception raised when asking for a result from a computation that has not been performed."""
    pass
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
