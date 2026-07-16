# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from collections import defaultdict, deque
from collections.abc import Mapping
import copy
import dataclasses
import cloudpickle
import os
import uuid


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
class TaskOutputHandle:
    """Symbolic handle to a task result or a value projected from it."""

    __slots__ = ("_workflow_id", "_task_id", "_path")

    def __init__(self, task_id, path=(), *, workflow_id=None):
        self._workflow_id = workflow_id
        self._task_id = task_id
        self._path = tuple(path)

    @property
    def workflow_id(self):
        return self._workflow_id

    @property
    def task_id(self):
        return self._task_id

    @property
    def path(self):
        return self._path

    def __getitem__(self, key):
        # One [] operation always appends one path token. Consequently,
        # output["a"]["b"] is a two-level lookup while output[("a", "b")]
        # looks up one tuple dictionary key, matching normal Python semantics.
        return TaskOutputHandle(
            self._task_id,
            self._path + (key,),
            workflow_id=self._workflow_id,
        )

    def attr(self, name):
        """Select an attribute from this output without overloading dictionary lookup."""
        if not isinstance(name, str):
            raise TypeError("attribute name must be a string")
        return TaskOutputHandle(
            self._task_id,
            self._path + (_TaskOutputAttribute(name),),
            workflow_id=self._workflow_id,
        )


@dataclasses.dataclass(frozen=True)
class _TaskOutputAttribute:
    name: str


class FileHandle:
    """Symbolic handle to an external input or a task-produced file."""

    __slots__ = ("_workflow_id", "_file_id")

    def __init__(self, workflow_id, file_id):
        self._workflow_id = workflow_id
        self._file_id = file_id

    @property
    def workflow_id(self):
        return self._workflow_id

    @property
    def file_id(self):
        return self._file_id

    def __repr__(self):
        return f"FileHandle(id={self._file_id})"


class TaskHandle:
    """User-facing symbolic handle returned by Workflow.add_task()."""

    __slots__ = ("_workflow", "_task_id")

    def __init__(self, workflow, task_id):
        self._workflow = workflow
        self._task_id = task_id

    def output(self):
        """Return a symbolic reference to this task's Python result."""
        return TaskOutputHandle(self._task_id, workflow_id=self._workflow._workflow_id)

    def file(self, path):
        """Return a handle for a file written in this task's sandbox."""
        return self._workflow._declare_task_file(self, path)

    def __repr__(self):
        return f"TaskHandle(id={self._task_id})"


# The Workflow is a directed acyclic graph (DAG) that represents the logical dependencies between tasks.
# It is used to build the C executor graph.
class Workflow:

    _LEAF_TYPES = (str, bytes, bytearray, memoryview, int, float, bool, type(None))

    def __init__(self):
        self._workflow_id = uuid.uuid4().hex
        self._next_task_id = 1
        self._next_file_id = 1
        self.callables = []
        self._callable_index = {}

        self.task_dict = {}

        self.parents_of = defaultdict(set)     # workflow_key -> set of workflow_keys
        self.children_of = defaultdict(set)    # workflow_key -> set of workflow_keys

        self.input_files = {}                  # file_id -> absolute frontend path
        self.output_files = {}                 # file_id -> (producer task id, task-relative path)
        self.output_files_by_task = defaultdict(dict)  # task id -> relative path -> file_id
        self.file_consumers = defaultdict(set) # file_id -> consumer task ids
        self._local_execute = False
        self._local_file_paths = {}

        self.outfile_remote_name = defaultdict(lambda: None)   # workflow_key -> remote outfile name, will be set by the executor graph

        self.task_id_to_scheduler_key = {}                  # workflow_key -> scheduler key (C node id)
        self.scheduler_key_to_task_id = {}                  # scheduler key -> workflow_key

        self.extra_task_output_size_mb = {}  # workflow_key -> extra size in MB
        self.extra_task_sleep_time = {}      # workflow_key -> extra sleep time in seconds

    def _intern_callable(self, func):
        idx = self._callable_index.get(func)
        if idx is None:
            idx = len(self.callables)
            self.callables.append(func)
            self._callable_index[func] = idx
        return idx

    def _visit_task_output_refs(self, obj, on_ref, *, rewrite: bool, on_file=None):
        memo = {}
        active_immutable = set()

        def dict_key_contains_ref(key, seen):
            if isinstance(key, (TaskOutputHandle, TaskHandle, FileHandle)):
                return True
            if key is None or isinstance(key, self._LEAF_TYPES):
                return False
            oid = id(key)
            if oid in seen:
                return False
            seen.add(oid)
            if dataclasses.is_dataclass(key) and not isinstance(key, type):
                return any(dict_key_contains_ref(getattr(key, f.name), seen) for f in dataclasses.fields(key))
            if isinstance(key, Mapping):
                return any(
                    dict_key_contains_ref(k, seen) or dict_key_contains_ref(v, seen)
                    for k, v in key.items()
                )
            if isinstance(key, (list, tuple, set, frozenset, deque)):
                return any(dict_key_contains_ref(v, seen) for v in key)
            try:
                state = vars(key)
            except TypeError:
                state = None
            if state and any(dict_key_contains_ref(v, seen) for v in state.values()):
                return True
            for slot in getattr(type(key), "__slots__", ()):
                if isinstance(slot, str) and hasattr(key, slot):
                    if dict_key_contains_ref(getattr(key, slot), seen):
                        return True
            return False

        def rec(x):
            if isinstance(x, TaskOutputHandle):
                return on_ref(x)

            if isinstance(x, FileHandle):
                if on_file is None:
                    raise TypeError("FileHandle is not supported in this graph conversion")
                return on_file(x)

            if isinstance(x, TaskHandle):
                raise TypeError("TaskHandle cannot be used as an argument directly; use task.output()")

            if x is None or isinstance(x, self._LEAF_TYPES):
                return x if rewrite else None

            oid = id(x)
            if oid in memo:
                return memo[oid] if rewrite else None

            if not rewrite:
                memo[oid] = None

            if isinstance(x, Mapping):
                for k in x.keys():
                    if dict_key_contains_ref(k, set()):
                        raise ValueError("dependency handles cannot be used as dict keys")
                if not rewrite:
                    for v in x.values():
                        rec(v)
                    return None

                # copy+clear preserves dict subclasses and defaultdict factories,
                # while publishing the empty object first preserves aliases/cycles.
                try:
                    out = copy.copy(x)
                    out.clear()
                except Exception:
                    out = {}
                memo[oid] = out
                for k, v in x.items():
                    out[k] = rec(v)
                return out

            if isinstance(x, list):
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                out = []
                memo[oid] = out
                out.extend(rec(v) for v in x)
                return out

            if isinstance(x, deque):
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                out = deque(maxlen=x.maxlen)
                memo[oid] = out
                out.extend(rec(v) for v in x)
                return out

            if isinstance(x, set):
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                out = set()
                memo[oid] = out
                out.update(rec(v) for v in x)
                return out

            if isinstance(x, tuple) and hasattr(x, "_fields"):  # namedtuple
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                if oid in active_immutable:
                    raise ValueError("cyclic dependency containers involving namedtuple are not supported")
                active_immutable.add(oid)
                try:
                    out = x.__class__(*(rec(v) for v in x))
                finally:
                    active_immutable.remove(oid)
                memo[oid] = out
                return out

            if isinstance(x, (tuple, frozenset)):
                if not rewrite:
                    for v in x:
                        rec(v)
                    return None
                if oid in active_immutable:
                    raise ValueError("cyclic dependency containers involving immutable containers are not supported")
                active_immutable.add(oid)
                try:
                    values = [rec(v) for v in x]
                    out = tuple(values) if isinstance(x, tuple) else frozenset(values)
                finally:
                    active_immutable.remove(oid)
                memo[oid] = out
                return out

            if dataclasses.is_dataclass(x) and not isinstance(x, type):
                if not rewrite:
                    for field in dataclasses.fields(x):
                        rec(getattr(x, field.name))
                    return None
                out = copy.copy(x)
                memo[oid] = out
                for field in dataclasses.fields(x):
                    object.__setattr__(out, field.name, rec(getattr(x, field.name)))
                return out

            if dict_key_contains_ref(x, set()):
                raise TypeError(
                    "dependency handle inside an arbitrary custom object is not supported; "
                    "use a dataclass or a supported container"
                )

            return x if rewrite else None

        return rec(obj)

    def _find_dependencies(self, obj):
        parents = set()
        files = set()

        def on_ref(r):
            if r.workflow_id is not None and r.workflow_id != self._workflow_id:
                raise ValueError("task output belongs to a different Workflow")
            parents.add(r.task_id)
            return None

        def on_file(f):
            if f.workflow_id != self._workflow_id:
                raise ValueError("file belongs to a different Workflow")
            if f.file_id not in self.input_files and f.file_id not in self.output_files:
                raise ValueError("file does not belong to this Workflow")
            files.add(f.file_id)
            if f.file_id in self.output_files:
                producer_task_id = self.output_files[f.file_id][0]
                parents.add(producer_task_id)
            return None

        self._visit_task_output_refs(obj, on_ref, rewrite=False, on_file=on_file)
        return parents, files

    def _allocate_task_id(self):
        while self._next_task_id in self.task_dict:
            self._next_task_id += 1
        task_id = self._next_task_id
        self._next_task_id += 1
        return task_id

    def _add_task_with_key(self, workflow_key, func, *args, **kwargs):
        if workflow_key in self.task_dict:
            raise ValueError(f"Task {workflow_key} already exists")
        if not callable(func):
            raise TypeError("task function must be callable")

        func_id = self._intern_callable(func)
        self.task_dict[workflow_key] = (func_id, args, kwargs)

        parents, files = self._find_dependencies((args, kwargs))

        for parent in parents:
            self.parents_of[workflow_key].add(parent)
            self.children_of[parent].add(workflow_key)

        for file_id in files:
            self.file_consumers[file_id].add(workflow_key)

        return TaskHandle(self, workflow_key)

    def add_task(self, func, *args, **kwargs):
        """Add a task with a private generated id and return its TaskHandle."""
        if not callable(func):
            raise TypeError("add_task expects a callable followed by its arguments")
        return self._add_task_with_key(self._allocate_task_id(), func, *args, **kwargs)

    def _task_key(self, task):
        if not isinstance(task, TaskHandle):
            raise TypeError("expected a TaskHandle from this Workflow")
        if task._workflow is not self:
            raise ValueError("task belongs to a different Workflow")
        return task._task_id

    def _task_handle(self, task_id):
        """Return a handle for an internal/adaptor task id."""
        if task_id not in self.task_dict:
            raise KeyError(f"Task {task_id} does not exist")
        return TaskHandle(self, task_id)

    def _allocate_file_id(self):
        file_id = self._next_file_id
        self._next_file_id += 1
        return file_id

    def file(self, path):
        """Return a handle for an existing file in the frontend filesystem."""
        source_path = os.fspath(path)
        if not isinstance(source_path, str):
            raise TypeError("input file path must be a string or path-like object")
        if "\0" in source_path:
            raise ValueError("input file path contains a null byte")
        source_path = os.path.abspath(source_path)
        if not os.path.isfile(source_path):
            raise FileNotFoundError(source_path)
        file_id = self._allocate_file_id()
        self.input_files[file_id] = source_path
        return FileHandle(self._workflow_id, file_id)

    def _declare_task_file(self, task, path):
        task_id = self._task_key(task)
        path = os.fspath(path)
        if not isinstance(path, str):
            raise TypeError("output file path must be a string or path-like object")
        if "\0" in path:
            raise ValueError("output file path contains a null byte")
        normalized = os.path.normpath(path)
        if not path or normalized in ("", ".") or os.path.isabs(path) or normalized == ".." or normalized.startswith("../"):
            raise ValueError("output file path must be a non-empty relative path inside the task sandbox")
        if normalized in self.output_files_by_task[task_id]:
            raise ValueError(f"task already declares output file {normalized!r}")
        file_id = self._allocate_file_id()
        self.output_files[file_id] = (task_id, normalized)
        self.output_files_by_task[task_id][normalized] = file_id
        return FileHandle(self._workflow_id, file_id)

    def file_input_path(self, file_id):
        """Resolve a FileHandle to the path visible to the current task."""
        if self._local_execute:
            if file_id in self._local_file_paths:
                return self._local_file_paths[file_id]
            if file_id in self.input_files:
                return self.input_files[file_id]
            raise RuntimeError(f"file {file_id} is not available yet")
        if file_id in self.input_files:
            base = os.path.basename(self.input_files[file_id])
        else:
            base = os.path.basename(self.output_files[file_id][1])
        return f"vine-graph-file-{file_id}-{base}"

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

    def sink_tasks(self):
        """Return handles for tasks with no downstream children."""
        return [self._task_handle(key) for key in self.task_dict if not self.children_of.get(key)]

    def finalize(self):
        """Finalize the workflow. Dependencies are recorded when tasks are added."""
