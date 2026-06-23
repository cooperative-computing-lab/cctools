# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import is_dataclass, fields, replace
import cloudpickle


# Lightweight wrapper around task results that optionally pads the payload. The
# padding lets tests model large outputs without altering the logical result.
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


# A reference to a task output. This is used to represent the output of a task as a dependency of another task.
class TaskOutputRef:
    __slots__ = ("workflow_key", "path")

    def __init__(self, workflow_key, path=()):
        self.workflow_key = workflow_key
        self.path = tuple(path)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return TaskOutputRef(self.workflow_key, self.path + key)
        return TaskOutputRef(self.workflow_key, self.path + (key,))


# The Workflow is a directed acyclic graph (DAG) that represents the logical dependencies between tasks.
# It is used to build the C executor graph.
class Workflow:

    _LEAF_TYPES = (str, bytes, bytearray, memoryview, int, float, bool, type(None))

    def __init__(self):
        self.callables = []
        self._callable_index = {}

        self.task_dict = {}

        self.parents_of = defaultdict(set)     # workflow_key -> set of workflow_keys
        self.children_of = defaultdict(set)    # workflow_key -> set of workflow_keys

        self.producer_of = {}                  # filename -> workflow_key
        self.consumers_of = defaultdict(set)   # filename -> set of workflow_keys

        self.outfile_remote_name = defaultdict(lambda: None)   # workflow_key -> remote outfile name, will be set by the executor graph

        self.workflow_key_to_scheduler_key = {}                  # workflow_key -> scheduler key (C node id)
        self.scheduler_key_to_workflow_key = {}                  # scheduler key -> workflow_key

        self.extra_task_output_size_mb = {}  # workflow_key -> extra size in MB
        self.extra_task_sleep_time = {}      # workflow_key -> extra sleep time in seconds

    def _intern_callable(self, func):
        idx = self._callable_index.get(func)
        if idx is None:
            idx = len(self.callables)
            self.callables.append(func)
            self._callable_index[func] = idx
        return idx

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
            parents.add(r.workflow_key)
            return None

        self._visit_task_output_refs(obj, on_ref, rewrite=False)
        return parents

    def add_task(self, workflow_key, func, *args, **kwargs):
        if workflow_key in self.task_dict:
            raise ValueError(f"Task {workflow_key} already exists")

        func_id = self._intern_callable(func)
        self.task_dict[workflow_key] = (func_id, args, kwargs)

        parents = self._find_parents(args) | self._find_parents(kwargs)

        for parent in parents:
            self.parents_of[workflow_key].add(parent)
            self.children_of[parent].add(workflow_key)

    def task_produces(self, workflow_key, *filenames):
        for filename in filenames:
            # a file can only be produced by one task
            if filename in self.producer_of:
                raise ValueError(f"File {filename} already produced by task {self.producer_of[filename]}")
            self.producer_of[filename] = workflow_key

    def task_consumes(self, workflow_key, *filenames):
        for filename in filenames:
            # a file can be consumed by multiple tasks
            self.consumers_of[filename].add(workflow_key)

    def save_task_output(self, workflow_key, output):
        with open(self.outfile_remote_name[workflow_key], "wb") as f:
            wrapped_output = TaskOutputWrapper(output, extra_size_mb=self.extra_task_output_size_mb[workflow_key])
            cloudpickle.dump(wrapped_output, f)

    def load_task_output(self, workflow_key):
        return TaskOutputWrapper.load_from_path(self.outfile_remote_name[workflow_key])

    def get_topological_order(self):
        indegree = {}
        for workflow_key in self.task_dict:
            indegree[workflow_key] = len(self.parents_of.get(workflow_key, ()))

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

    def finalize(self):
        # build the dependencies determined by files produced and consumed
        for file, producer in self.producer_of.items():
            for consumer in self.consumers_of.get(file, ()):
                self.parents_of[consumer].add(producer)
                self.children_of[producer].add(consumer)
