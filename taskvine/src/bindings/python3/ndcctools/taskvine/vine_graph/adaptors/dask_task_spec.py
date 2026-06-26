import hashlib
from collections.abc import Mapping

from .dask_common import build_task_expr, identity, resolve_graph_key_if_task
from ..workflow import TaskOutputRef


def _safe_repr(value, limit=800):
    """Best-effort repr that stays compact for large graphs."""
    try:
        text = repr(value)
    except Exception as exc:
        text = f"<unreprable {type(value).__name__}: {exc}>"
    if limit and len(text) > limit:
        return text[:limit] + "...<truncated>"
    return text


def _extract_callable_from_task(node):
    for attr in ("function", "op", "callable", "func", "operation", "callable_obj"):
        if not hasattr(node, attr):
            continue
        value = getattr(node, attr)
        if value is not None and (callable(value) or hasattr(value, "__call__")):
            return value
    if hasattr(node, "__call__") and callable(node):
        return node
    return None


def _is_pure_value_op(func):
    return func in (dict, list, tuple, set, frozenset)


def _is_identity_cast_op(func):
    name = getattr(func, "__name__", None)
    module = getattr(func, "__module__", None)
    return bool(name == "_identity_cast" and module and module.startswith("dask"))


def _is_too_large_inline_value(value, *, max_container_len=2000):
    try:
        return isinstance(value, (list, tuple, set, frozenset, dict)) and len(value) > max_container_len
    except Exception:
        return False


class DaskTaskSpecConverter:
    """Convert modern Dask TaskSpec nodes into Workflow task expressions."""

    def __init__(self, dts_module):
        self.dts = dts_module
        self.lifted_nodes = {}
        self._lift_cache = {}
        self._lift_counter = 0

    def is_node(self, value):
        if not self.dts:
            return False
        try:
            return isinstance(value, self.dts.GraphNode)
        except AttributeError:
            return False

    def pending_nodes(self, converted):
        return [(key, node) for key, node in self.lifted_nodes.items() if key not in converted]

    def convert_node(self, key, node, workflow_keys):
        if not self.dts:
            raise RuntimeError("Dask TaskSpec support unavailable: dask._task_spec is not installed")

        task_cls = getattr(self.dts, "Task", None)
        alias_cls = getattr(self.dts, "Alias", None)
        literal_cls = getattr(self.dts, "Literal", None)
        datanode_cls = getattr(self.dts, "DataNode", None)
        nested_cls = getattr(self.dts, "NestedContainer", None)
        taskref_cls = getattr(self.dts, "TaskRef", None)

        if task_cls and isinstance(node, task_cls):
            return self._convert_task_node(key, node, workflow_keys)
        if alias_cls and isinstance(node, alias_cls):
            alias_ref = self._extract_alias_target(node, workflow_keys)
            if alias_ref is None:
                raise ValueError(f"Alias {key} is missing a resolvable upstream task")
            return build_task_expr(identity, [alias_ref], {})
        if datanode_cls and isinstance(node, datanode_cls):
            return build_task_expr(identity, [node.value], {})
        if literal_cls and isinstance(node, literal_cls):
            return build_task_expr(identity, [node.value], {})
        if taskref_cls and isinstance(node, taskref_cls):
            return build_task_expr(identity, [TaskOutputRef(node.key, getattr(node, "path", ()) or ())], {})
        if nested_cls and isinstance(node, nested_cls):
            payload = getattr(node, "value", None)
            if payload is None:
                payload = getattr(node, "data", None)
            return build_task_expr(identity, [self.unwrap_operand(payload, workflow_keys, parent_key=key)], {})
        return build_task_expr(identity, [node], {})

    def _convert_task_node(self, key, node, workflow_keys):
        func = _extract_callable_from_task(node)
        if func is None:
            raise TypeError(f"Task {key} is missing a callable function/op attribute")

        raw_args = getattr(node, "args", ()) or ()
        raw_kwargs = getattr(node, "kwargs", {}) or {}

        args = []
        try:
            for index, arg in enumerate(raw_args):
                args.append(self.unwrap_operand(arg, workflow_keys, parent_key=key))
        except Exception as exc:
            raise TypeError(
                "Failed to adapt TaskSpec node argument while converting to Workflow.\n"
                f"- parent_workflow_key: {key!r}\n"
                f"- func: {_safe_repr(func)}\n"
                f"- arg_index: {index}\n"
                f"- arg_value: {_safe_repr(arg)}\n"
                f"- raw_args: {_safe_repr(raw_args)}\n"
                f"- raw_kwargs: {_safe_repr(raw_kwargs)}"
            ) from exc

        kwargs = {}
        try:
            for kwarg_key, value in raw_kwargs.items():
                kwargs[kwarg_key] = self.unwrap_operand(value, workflow_keys, parent_key=key)
        except Exception as exc:
            raise TypeError(
                "Failed to adapt TaskSpec node kwarg while converting to Workflow.\n"
                f"- parent_workflow_key: {key!r}\n"
                f"- func: {_safe_repr(func)}\n"
                f"- kwarg_key: {kwarg_key!r}\n"
                f"- kwarg_value: {_safe_repr(value)}\n"
                f"- raw_args: {_safe_repr(raw_args)}\n"
                f"- raw_kwargs: {_safe_repr(raw_kwargs)}"
            ) from exc

        return build_task_expr(func, args, kwargs)

    def unwrap_operand(self, operand, workflow_keys, *, parent_key=None):
        taskref_cls = getattr(self.dts, "TaskRef", None)
        if taskref_cls and isinstance(operand, taskref_cls):
            return TaskOutputRef(getattr(operand, "key", None), getattr(operand, "path", ()) or ())

        alias_cls = getattr(self.dts, "Alias", None)
        if alias_cls and isinstance(operand, alias_cls):
            alias_ref = self._extract_alias_target(operand, workflow_keys)
            if alias_ref is None:
                raise ValueError("Alias node is missing a valid upstream source")
            return alias_ref

        literal_cls = getattr(self.dts, "Literal", None)
        if literal_cls and isinstance(operand, literal_cls):
            return getattr(operand, "value", None)

        datanode_cls = getattr(self.dts, "DataNode", None)
        if datanode_cls and isinstance(operand, datanode_cls):
            return operand.value

        nested_cls = getattr(self.dts, "NestedContainer", None)
        if nested_cls and isinstance(operand, nested_cls):
            payload = getattr(operand, "value", None)
            if payload is None:
                payload = getattr(operand, "data", None)
            return self.unwrap_operand(payload, workflow_keys, parent_key=parent_key)

        task_cls = getattr(self.dts, "Task", None)
        if task_cls and isinstance(operand, task_cls):
            return self._unwrap_task_operand(operand, workflow_keys, parent_key=parent_key)

        if isinstance(operand, list):
            return [self.unwrap_operand(value, workflow_keys, parent_key=parent_key) for value in operand]
        if isinstance(operand, tuple):
            return tuple(self.unwrap_operand(value, workflow_keys, parent_key=parent_key) for value in operand)
        if isinstance(operand, Mapping):
            return {key: self.unwrap_operand(value, workflow_keys, parent_key=parent_key) for key, value in operand.items()}
        if isinstance(operand, set):
            return {self.unwrap_operand(value, workflow_keys, parent_key=parent_key) for value in operand}
        if isinstance(operand, frozenset):
            return frozenset(self.unwrap_operand(value, workflow_keys, parent_key=parent_key) for value in operand)
        return operand

    def _unwrap_task_operand(self, operand, workflow_keys, *, parent_key=None):
        inline_key = getattr(operand, "key", None)
        if inline_key is not None and inline_key in workflow_keys:
            return TaskOutputRef(inline_key, ())

        func = _extract_callable_from_task(operand)
        if func is None:
            return self._lift_inline_task(operand, workflow_keys, parent_key=parent_key)
        if _is_identity_cast_op(func):
            return self._unwrap_identity_cast(operand, workflow_keys, parent_key=parent_key)
        if _is_pure_value_op(func):
            reduced, used_lift = self._reduce_inline_task(operand, workflow_keys, parent_key=parent_key)
            if used_lift or _is_too_large_inline_value(reduced):
                return self._lift_inline_task(operand, workflow_keys, parent_key=parent_key)
            return reduced
        return self._lift_inline_task(operand, workflow_keys, parent_key=parent_key)

    def _unwrap_identity_cast(self, operand, workflow_keys, *, parent_key=None):
        raw_args = getattr(operand, "args", ()) or ()
        raw_kwargs = getattr(operand, "kwargs", {}) or {}
        typ = raw_kwargs.get("typ", None)
        values = [self.unwrap_operand(arg, workflow_keys, parent_key=parent_key) for arg in raw_args]

        if typ in (list, tuple, set, frozenset, dict):
            try:
                return typ(values)
            except Exception:
                pass
        return self._lift_inline_task(operand, workflow_keys, parent_key=parent_key)

    def _extract_alias_target(self, alias_node, workflow_keys):
        fields = getattr(alias_node.__class__, "__dataclass_fields__", {}) if self.dts else {}
        path = tuple(getattr(alias_node, "path", ()) or ())

        for candidate in ("alias_of", "target", "source", "ref"):
            if candidate not in fields:
                continue
            key = resolve_graph_key_if_task(getattr(alias_node, candidate, None), workflow_keys)
            if key is not None:
                return TaskOutputRef(key, path)

        deps = getattr(alias_node, "dependencies", None)
        if deps:
            deps = list(deps)
            if len(deps) == 1:
                key = resolve_graph_key_if_task(deps[0], workflow_keys)
                return TaskOutputRef(key if key is not None else deps[0], path)
        return None

    def _reduce_inline_task(self, task_node, workflow_keys, *, parent_key=None):
        func = _extract_callable_from_task(task_node)
        raw_args = getattr(task_node, "args", ()) or ()
        raw_kwargs = getattr(task_node, "kwargs", {}) or {}
        used_lift = False

        args = []
        for arg in raw_args:
            before = len(self.lifted_nodes)
            args.append(self.unwrap_operand(arg, workflow_keys, parent_key=parent_key))
            used_lift = used_lift or (len(self.lifted_nodes) != before)

        kwargs = {}
        for key, value in raw_kwargs.items():
            before = len(self.lifted_nodes)
            kwargs[key] = self.unwrap_operand(value, workflow_keys, parent_key=parent_key)
            used_lift = used_lift or (len(self.lifted_nodes) != before)

        try:
            return func(*args, **kwargs), used_lift
        except Exception:
            return self._lift_inline_task(task_node, workflow_keys, parent_key=parent_key), True

    def _lift_inline_task(self, task_node, workflow_keys, *, parent_key=None):
        inline_key = getattr(task_node, "key", None)
        if parent_key is not None and inline_key == parent_key:
            raise ValueError(f"Refusing to lift Task that would self-reference parent key {parent_key!r}")

        signature = self._structural_signature(task_node, workflow_keys)
        cached = self._lift_cache.get(signature)
        if cached is not None:
            return TaskOutputRef(cached, ())

        digest = hashlib.sha1(signature.encode("utf-8")).hexdigest()[:16]
        base = f"__lift__{digest}"
        new_key = base
        while new_key in workflow_keys or new_key in self.lifted_nodes:
            self._lift_counter += 1
            new_key = f"{base}_{self._lift_counter}"

        self._lift_cache[signature] = new_key
        self.lifted_nodes[new_key] = task_node
        workflow_keys.add(new_key)
        return TaskOutputRef(new_key, ())

    def _structural_signature(self, obj, workflow_keys):
        try:
            return self._structural_signature_impl(obj, workflow_keys)
        except Exception:
            return f"fallback({_safe_repr(obj)})"

    def _structural_signature_impl(self, obj, workflow_keys):
        taskref_cls = getattr(self.dts, "TaskRef", None)
        alias_cls = getattr(self.dts, "Alias", None)
        literal_cls = getattr(self.dts, "Literal", None)
        datanode_cls = getattr(self.dts, "DataNode", None)
        nested_cls = getattr(self.dts, "NestedContainer", None)
        task_cls = getattr(self.dts, "Task", None)

        if taskref_cls and isinstance(obj, taskref_cls):
            return f"TaskRef({getattr(obj, 'key', None)!r},{tuple(getattr(obj, 'path', ()) or ())!r})"
        if alias_cls and isinstance(obj, alias_cls):
            ref = self._extract_alias_target(obj, workflow_keys)
            return f"Alias({getattr(ref, 'workflow_key', None)!r},{getattr(ref, 'path', ())!r})"
        if literal_cls and isinstance(obj, literal_cls):
            return f"Literal({_safe_repr(getattr(obj, 'value', None))})"
        if datanode_cls and isinstance(obj, datanode_cls):
            return f"DataNode({_safe_repr(getattr(obj, 'value', None))})"
        if nested_cls and isinstance(obj, nested_cls):
            payload = getattr(obj, "value", None)
            if payload is None:
                payload = getattr(obj, "data", None)
            return f"Nested({self._structural_signature(payload, workflow_keys)})"
        if task_cls and isinstance(obj, task_cls):
            key = getattr(obj, "key", None)
            if key is not None and key in workflow_keys:
                return f"TaskKey({key!r})"
            func = _extract_callable_from_task(obj)
            func_id = (getattr(func, "__module__", None), getattr(func, "__qualname__", None), getattr(func, "__name__", None))
            args = getattr(obj, "args", ()) or ()
            kwargs = getattr(obj, "kwargs", {}) or {}
            arg_sigs = ",".join(self._structural_signature(arg, workflow_keys) for arg in args)
            kw_sigs = ",".join(f"{key}={self._structural_signature(value, workflow_keys)}" for key, value in sorted(kwargs.items()))
            return f"TaskInline(func={func_id!r},args=[{arg_sigs}],kwargs=[{kw_sigs}])"
        if isinstance(obj, list):
            return "list(" + ",".join(self._structural_signature(value, workflow_keys) for value in obj) + ")"
        if isinstance(obj, tuple):
            return "tuple(" + ",".join(self._structural_signature(value, workflow_keys) for value in obj) + ")"
        if isinstance(obj, dict):
            items = ",".join(
                f"{_safe_repr(key)}:{self._structural_signature(value, workflow_keys)}"
                for key, value in sorted(obj.items(), key=lambda item: repr(item[0]))
            )
            return "dict(" + items + ")"
        if isinstance(obj, (set, frozenset)):
            items = ",".join(sorted(self._structural_signature(value, workflow_keys) for value in obj))
            return f"{type(obj).__name__}(" + items + ")"
        return f"py({_safe_repr(obj)})"
