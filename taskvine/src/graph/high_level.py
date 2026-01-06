"""High-level blueprint graph for TaskVine DAG modelling."""

from collections import defaultdict
import uuid


class ReturnRef:
    """Reference to the return value (or sub-path) of another task."""

    def __init__(self, node, path=()):
        if node is None:
            raise ValueError("node must not be None")
        self.node = node
        self.path = tuple(path)

    def derive(self, *path):
        return ReturnRef(self.node, self.path + tuple(path))

    def __getitem__(self, item):
        if isinstance(item, tuple):
            return self.derive(*item)
        return self.derive(item)

class BlueprintFile:
    """Describes a file that tasks may produce or consume."""

    def __init__(self, name, **metadata):
        if not name or not isinstance(name, str):
            raise ValueError("File name must be a non-empty string")
        self.name = name
        self.metadata = dict(metadata)

class BlueprintGraphTask:
    """Represents a task node within the blueprint graph."""

    def __init__(self, graph, func, args=None, kwargs=None, name=None):
        self.graph = graph
        self.func = func
        self.name = name or graph._generate_task_name()
        self.args = tuple(args) if args is not None else tuple()
        self.kwargs = dict(kwargs) if kwargs is not None else {}
        self.produced_files = []
        self.consumed_files = []
        self._return_ref = ReturnRef(self)

    def produces(self, *files):
        for file_obj in files:
            file_obj = self.graph._ensure_file(file_obj)
            self.graph._register_file_producer(self, file_obj)
            if file_obj not in self.produced_files:
                self.produced_files.append(file_obj)
        return self

    def consumes(self, *files):
        for file_obj in files:
            file_obj = self.graph._ensure_file(file_obj)
            self.graph._register_file_consumer(self, file_obj)
            if file_obj not in self.consumed_files:
                self.consumed_files.append(file_obj)
        return self

    def ret(self):
        return self._return_ref

    def update_kwargs(self, **new_kwargs):
        if not new_kwargs:
            return self
        merged = dict(self.kwargs)
        merged.update(new_kwargs)
        self.kwargs = merged
        self.graph.invalidate()
        return self

class BlueprintGraph:
    """Blueprint graph supporting ReturnRef and file dependencies."""

    _SUPPORTED_SCALARS = (str, int, float, bool, type(None), bytes)
    def __init__(self):
        self._tasks = []
        self._tasks_by_name = {}
        self._return_edges = defaultdict(set)
        self._files = {}
        self._file_producer = {}
        self._file_consumers = defaultdict(set)
        self._dirty = True

    def define_file(self, name, **metadata):
        existing = self._files.get(name)
        if existing:
            existing.metadata.update(metadata)
            return existing
        file_obj = BlueprintFile(name, **metadata)
        self._files[name] = file_obj
        return file_obj

    def task(self, func, *args, name=None, **kwargs):
        task = BlueprintGraphTask(
            self,
            func,
            args=args,
            kwargs=kwargs,
            name=name,
        )
        return self._register_task(task)

    def nodes(self):
        return list(self._tasks)

    def file_links(self):
        links = []
        for file_name, consumers in self._file_consumers.items():
            file_obj = self._files.get(file_name)
            if file_obj is None:
                continue
            producer = self._file_producer.get(file_name)
            for consumer in consumers:
                links.append((producer, consumer, file_obj))
        return links

    def return_children(self, task):
        task = self._ensure_task(task)
        return set(self._return_edges.get(task, set()))

    def invalidate(self):
        self._dirty = True

    def validate(self):
        if self._dirty:
            for task in self._tasks:
                self._validate_supported_value(task.args, f"{task.name}.args")
                self._validate_supported_value(task.kwargs, f"{task.name}.kwargs")
                self._assert_no_returnref_in_keys(task.args, f"{task.name}.args")
                self._assert_no_returnref_in_keys(task.kwargs, f"{task.name}.kwargs")
            self._rebuild_return_edges()
            self._dirty = False

        missing_producers = [
            file_name
            for file_name, consumers in self._file_consumers.items()
            if consumers and file_name not in self._file_producer
        ]
        if missing_producers:
            raise ValueError(
                f"Files consumed without producers: {', '.join(sorted(missing_producers))}"
            )

    def describe(self):
        self.validate()
        data = {
            "tasks": {},
            "return_links": [],
            "file_links": [],
            "files": {
                name: dict(file_obj.metadata) for name, file_obj in self._files.items()
            },
        }
        for task in self._tasks:
            data["tasks"][task.name] = {
                "func": getattr(task.func, "__name__", repr(task.func)),
                "args": self._serialise(task.args),
                "kwargs": self._serialise(task.kwargs),
                "produces": [self._serialise_file(f) for f in task.produced_files],
                "consumes": [self._serialise_file(f) for f in task.consumed_files],
            }
        for parent, children in self._return_edges.items():
            for child in children:
                data["return_links"].append(
                    {
                        "parent": parent.name,
                        "child": child.name,
                    }
                )
        for file_name, consumers in self._file_consumers.items():
            file_obj = self._files.get(file_name)
            if file_obj is None:
                continue
            producer = self._file_producer.get(file_name)
            for consumer in consumers:
                data["file_links"].append(
                    {
                        "parent": producer.name if producer else None,
                        "child": consumer.name if consumer else None,
                        "filename": file_obj.name,
                        "metadata": dict(file_obj.metadata),
                    }
                )
        return data

    def _register_task(self, task):
        canonical = self._tasks_by_name.get(task.name)
        if canonical is not None:
            raise ValueError(f"Task name '{task.name}' already registered")
        self._tasks.append(task)
        self._tasks_by_name[task.name] = task
        self.invalidate()
        return task

    def _register_file_producer(self, task, file_obj):
        task = self._ensure_task(task)
        existing = self._file_producer.get(file_obj.name)
        if existing and existing is not task:
            raise ValueError(
                f"File '{file_obj.name}' already produced by task '{existing.name}'"
            )
        self._file_producer[file_obj.name] = task
        self.invalidate()

    def _register_file_consumer(self, task, file_obj):
        task = self._ensure_task(task)
        self._file_consumers[file_obj.name].add(task)
        self.invalidate()

    def _ensure_file(self, file_obj):
        if isinstance(file_obj, BlueprintFile):
            existing = self._files.get(file_obj.name)
            if existing:
                existing.metadata.update(file_obj.metadata)
                return existing
            self._files[file_obj.name] = file_obj
            return file_obj
        if isinstance(file_obj, str):
            existing = self._files.get(file_obj)
            if not existing:
                existing = BlueprintFile(file_obj)
                self._files[file_obj] = existing
            return existing
        raise TypeError("File reference must be BlueprintFile or string name")

    def _rebuild_return_edges(self):
        self._return_edges.clear()
        for task in self._tasks:
            canonical_task = self._ensure_task(task)
            parents = self._collect_return_refs(task.args) | self._collect_return_refs(task.kwargs)
            for parent in parents:
                canonical_parent = self._ensure_task(parent)
                self._return_edges[canonical_parent].add(canonical_task)

    def _collect_return_refs(self, value):
        refs = set()
        if isinstance(value, ReturnRef):
            refs.add(value.node)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                refs |= self._collect_return_refs(item)
        elif isinstance(value, dict):
            for item in value.values():
                refs |= self._collect_return_refs(item)
        return refs

    def _serialise(self, value):
        if isinstance(value, ReturnRef):
            return {
                "type": "return_ref",
                "node": value.node.name,
                "path": list(value.path),
            }
        if isinstance(value, (list, tuple)):
            return [self._serialise(item) for item in value]
        if isinstance(value, (set, frozenset)):
            items = [self._serialise(item) for item in value]
            try:
                return sorted(items, key=lambda x: str(x))
            except TypeError:
                return items
        if isinstance(value, dict):
            return {k: self._serialise(v) for k, v in value.items()}
        return value

    def _serialise_file(self, file_obj):
        return {
            "name": file_obj.name,
            "metadata": dict(file_obj.metadata),
        }

    def _ensure_task(self, task):
        name = task.name if isinstance(task, BlueprintGraphTask) else str(task)
        canonical = self._tasks_by_name.get(name)
        if canonical is None:
            raise ValueError(f"Task '{name}' does not belong to this graph")
        return canonical

    def _generate_task_name(self):
        return uuid.uuid4().hex

    def _validate_supported_value(self, value, context, _seen=None):
        if _seen is None:
            _seen = set()
        value_id = id(value)
        if value_id in _seen:
            return
        _seen.add(value_id)

        if isinstance(value, ReturnRef):
            return
        if isinstance(value, self._SUPPORTED_SCALARS):
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                self._validate_supported_value(item, context, _seen)
            return
        if isinstance(value, dict):
            for item in value.values():
                self._validate_supported_value(item, context, _seen)
            return

        raise TypeError(
            f"Unsupported argument type '{type(value).__name__}' encountered in {context}. "
            "Wrap custom objects in basic containers (dict/list/tuple/set) or convert them "
            "to ReturnRef before building the graph."
        )

    def _assert_no_returnref_in_keys(self, value, context, _seen=None):
        if _seen is None:
            _seen = set()
        value_id = id(value)
        if value_id in _seen:
            return
        _seen.add(value_id)

        if isinstance(value, dict):
            for key, val in value.items():
                if isinstance(key, ReturnRef):
                    raise TypeError(
                        f"ReturnRef cannot be used as a dict key in {context}"
                    )
                self._assert_no_returnref_in_keys(val, context, _seen)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for item in value:
                self._assert_no_returnref_in_keys(item, context, _seen)



