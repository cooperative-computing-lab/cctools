# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import is_dataclass, fields, replace
import cloudpickle


class TaskOutputWrapper:
    def __init__(self, result, extra_size_mb=None):
        self.result = result
        self.extra_obj = bytearray(int(extra_size_mb * 1024 * 1024)) if extra_size_mb and extra_size_mb > 0 else None

    @staticmethod
    def load_from_path(path):
        try:
            with open(path, "rb") as f:
                result_obj = cloudpickle.load(f)
                assert isinstance(result_obj, TaskOutputWrapper), "Loaded object is not of type TaskOutputWrapper"
                return result_obj.result
        except FileNotFoundError:
            raise FileNotFoundError(f"Task result file not found at {path}")


class TaskOutputRef:
    __slots__ = ("task_key", "path")

    def __init__(self, task_key, path=()):
        self.task_key = task_key
        self.path = tuple(path)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return TaskOutputRef(self.task_key, self.path + key)
        return TaskOutputRef(self.task_key, self.path + (key,))


class BlueprintGraph:

    _LEAF_TYPES = (str, bytes, bytearray, memoryview, int, float, bool, type(None))

    def __init__(self):
        self.task_dict = {}                    # task_key -> (func, frozen_args, frozen_kwargs)

        self.parents_of = defaultdict(set)     # task_key -> set of task_keys
        self.children_of = defaultdict(set)    # task_key -> set of task_keys

        self.producer_of = {}                  # filename -> task_key
        self.consumers_of = defaultdict(set)   # filename -> set of task_keys

        self.outfile_remote_name = defaultdict(lambda: None)   # task_key -> remote outfile name, will be set by vine graph

        self.pykey2cid = {}                  # py_key -> c_id
        self.cid2pykey = {}                  # c_id -> py_key

    def _visit_task_output_refs(self, obj, on_ref, *, rewrite: bool):
        seen = set()

        def rec(x):
            if isinstance(x, TaskOutputRef):
                return on_ref(x)

            if x is None or isinstance(x, self._LEAF_TYPES):
                return x if rewrite else None

            oid = id(x)
            if oid in seen:
                return x if rewrite else None
            seen.add(oid)

            if isinstance(x, Mapping):
                for k in x.keys():
                    if isinstance(k, TaskOutputRef):
                        raise ValueError("TaskOutputRef cannot be used as dict key")
                if not rewrite:
                    for v in x.values():
                        rec(v)
                    return None
                return {k: rec(v) for k, v in x.items()}

            if is_dataclass(x) and not isinstance(x, type):
                if not rewrite:
                    for f in fields(x):
                        rec(getattr(x, f.name))
                    return None
                updates = {f.name: rec(getattr(x, f.name)) for f in fields(x)}
                try:
                    return replace(x, **updates)
                except Exception:
                    return x.__class__(**updates)

            if isinstance(x, tuple) and hasattr(x, "_fields"):  # namedtuple
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                return x.__class__(*(rec(v) for v in x))

            if isinstance(x, (list, tuple, set, frozenset, deque)):
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                it = (rec(v) for v in x)
                if isinstance(x, list):
                    return list(it)
                if isinstance(x, tuple):
                    return tuple(it)
                if isinstance(x, set):
                    return set(it)
                if isinstance(x, frozenset):
                    return frozenset(it)
                return deque(it)

            return x if rewrite else None

        return rec(obj)

    def _find_parents(self, obj):
        parents = set()

        def on_ref(r):
            parents.add(r.task_key)
            return None

        self._visit_task_output_refs(obj, on_ref, rewrite=False)
        return parents

    def add_task(self, task_key, func, *args, **kwargs):
        if task_key in self.task_dict:
            raise ValueError(f"Task {task_key} already exists")

        self.task_dict[task_key] = (func, args, kwargs)

        parents = self._find_parents(args) | self._find_parents(kwargs)

        for parent in parents:
            self.parents_of[task_key].add(parent)
            self.children_of[parent].add(task_key)

    def task_produces(self, task_key, *filenames):
        for filename in filenames:
            # a file can only be produced by one task
            if filename in self.producer_of:
                raise ValueError(f"File {filename} already produced by task {self.producer_of[filename]}")
            self.producer_of[filename] = task_key

    def task_consumes(self, task_key, *filenames):
        for filename in filenames:
            # a file can be consumed by multiple tasks
            self.consumers_of[filename].add(task_key)

    def save_task_output(self, task_key, output):
        with open(self.outfile_remote_name[task_key], "wb") as f:
            wrapped_output = TaskOutputWrapper(output, extra_size_mb=0)
            cloudpickle.dump(wrapped_output, f)

    def load_task_output(self, task_key):
        return TaskOutputWrapper.load_from_path(self.outfile_remote_name[task_key])

    def get_topological_order(self):
        indegree = {}
        for task_key in self.task_dict:
            indegree[task_key] = len(self.parents_of.get(task_key, ()))

        q = deque(t for t, d in indegree.items() if d == 0)
        order = []

        while q:
            u = q.popleft()
            order.append(u)

            for v in self.children_of.get(u, ()):
                indegree[v] -= 1
                if indegree[v] == 0:
                    q.append(v)

        if len(order) != len(self.task_dict):
            raise ValueError("Graph has a cycle or missing dependencies")

        return order

    def verify_topo(g, topo):
        pos = {k: i for i, k in enumerate(topo)}
        for child, parents in g.parents_of.items():
            for p in parents:
                if pos[p] > pos[child]:
                    raise AssertionError(f"bad topo: parent {p} after child {child}")
        print("topo verified: ok")

    def finalize(self):
        for file, producer in self.producer_of.items():
            for consumer in self.consumers_of.get(file, ()):
                self.parents_of[consumer].add(producer)
                self.children_of[producer].add(consumer)
