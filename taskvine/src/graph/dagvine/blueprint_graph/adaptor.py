from collections.abc import Mapping

try:
    import dask
except ImportError:
    dask = None

try:
    from dask.base import is_dask_collection
except ImportError:
    is_dask_collection = None

try:
    import importlib

    dts = importlib.import_module("dask._task_spec")
except Exception:
    # Treat any import failure as "no TaskSpec support" (including environments
    # where the private module is unavailable or type-checkers can't resolve it).
    dts = None

from ndcctools.taskvine.dagvine.blueprint_graph.blueprint_graph import TaskOutputRef, BlueprintGraph


def _identity(value):
    """Tiny helper that just hands back whatever you pass in (e.g. `_identity(3)` -> 3)."""
    return value


class Adaptor:
    """Normalize user task inputs so `BlueprintGraph` can consume them without extra massaging."""

    _LEAF_TYPES = (str, bytes, bytearray, memoryview, int, float, bool, type(None))

    def __init__(self, task_dict):

        if isinstance(task_dict, BlueprintGraph):
            self.converted = task_dict
            return

        # TaskSpec-only state used to "lift" inline Tasks that cannot be reduced to
        # a pure Python value (or would be unsafe/expensive to inline).
        self._lifted_nodes = {}
        self._lift_cache = {}
        self._lift_counter = 0
        # Global shared key-set for the whole adaptation run (original keys + lifted keys).
        # IMPORTANT: TaskSpec conversion must always consult the same shared set so that
        # lifted keys remain visible across subsequent conversions/dedup/reference checks.
        self._task_keys = set()

        normalized = self._normalize_task_dict(task_dict)
        self.converted = self._convert_to_blueprint_tasks(normalized)

    def _normalize_task_dict(self, task_dict):
        """Collapse every supported input style into a classic `{key: sexpr or TaskSpec}` mapping."""
        from_dask_collection = bool(
            is_dask_collection and any(is_dask_collection(v) for v in task_dict.values())
        )

        if from_dask_collection:
            task_dict = self._dask_collections_to_task_dict(task_dict)
        else:
            # IMPORTANT: treat plain user dicts as DAGVine sexprs by default.
            # If we unconditionally run `dask._task_spec.convert_legacy_graph(...)` when
            # dts is available, Dask will interpret our "final Mapping is kwargs"
            # convention as a positional dict argument, breaking sexpr semantics.
            task_dict = dict(task_dict)

        # Only ask Dask to rewrite legacy graphs when we *know* the input came
        # from a Dask collection/HLG. This keeps classic DAGVine sexprs stable
        # even in environments where dask._task_spec is installed.
        if from_dask_collection and dts and hasattr(dts, "convert_legacy_graph"):
            task_dict = dts.convert_legacy_graph(task_dict)

        return task_dict

    def _convert_to_blueprint_tasks(self, task_dict):
        """Turn each normalized entry into the `(func, args, kwargs)` triple that BlueprintGraph expects."""
        if not task_dict:
            return {}

        converted = {}
        # Shared task key universe for TaskSpec lifting/dedup/reference decisions.
        # Keep this as a single shared set for the whole conversion.
        self._task_keys = set(task_dict.keys())
        task_keys = self._task_keys

        for key, value in task_dict.items():
            if self._is_dts_node(value):
                converted[key] = self._convert_dts_graph_node(key, value, task_keys)
            else:
                converted[key] = self._convert_sexpr_task(value, task_keys)

        # If any inline TaskSpec Tasks were lifted during conversion, convert them too.
        # We do this iteratively because lifting a node can expose further inline Tasks.
        while True:
            pending = [(k, v) for k, v in self._lifted_nodes.items() if k not in converted]
            if not pending:
                break
            for k, node in pending:
                converted[k] = self._convert_dts_graph_node(k, node, task_keys)

        return converted

    def _convert_dts_graph_node(self, key, node, task_keys):
        """Translate modern Dask TaskSpec graph nodes into blueprint expressions."""
        if not dts:
            raise RuntimeError("Dask TaskSpec support unavailable: dask._task_spec is not installed")

        task_cls = getattr(dts, "Task", None)
        alias_cls = getattr(dts, "Alias", None)
        literal_cls = getattr(dts, "Literal", None)
        datanode_cls = getattr(dts, "DataNode", None)
        nested_cls = getattr(dts, "NestedContainer", None)
        taskref_cls = getattr(dts, "TaskRef", None)

        if task_cls and isinstance(node, task_cls):
            func = self._extract_callable_from_task(node)
            if func is None:
                raise TypeError(f"Task {key} is missing a callable function/op attribute")

            raw_args = getattr(node, "args", ()) or ()
            raw_kwargs = getattr(node, "kwargs", {}) or {}

            args = []
            try:
                for i, arg in enumerate(raw_args):
                    args.append(self._unwrap_dts_operand(arg, task_keys, parent_key=key))
            except Exception as e:
                raise TypeError(
                    "Failed to adapt TaskSpec node argument while converting to BlueprintGraph.\n"
                    f"- parent_task_key: {key!r}\n"
                    f"- func: {self._safe_repr(func)}\n"
                    f"- arg_index: {i}\n"
                    f"- arg_value: {self._safe_repr(arg)}\n"
                    f"- raw_args: {self._safe_repr(raw_args)}\n"
                    f"- raw_kwargs: {self._safe_repr(raw_kwargs)}"
                ) from e

            kwargs = {}
            try:
                for k, v in raw_kwargs.items():
                    kwargs[k] = self._unwrap_dts_operand(v, task_keys, parent_key=key)
            except Exception as e:
                raise TypeError(
                    "Failed to adapt TaskSpec node kwarg while converting to BlueprintGraph.\n"
                    f"- parent_task_key: {key!r}\n"
                    f"- func: {self._safe_repr(func)}\n"
                    f"- kwarg_key: {k!r}\n"
                    f"- kwarg_value: {self._safe_repr(v)}\n"
                    f"- raw_args: {self._safe_repr(raw_args)}\n"
                    f"- raw_kwargs: {self._safe_repr(raw_kwargs)}"
                ) from e

            return self._build_expr(func, args, kwargs)

        if alias_cls and isinstance(node, alias_cls):
            alias_ref = self._extract_alias_target(node, task_keys)
            if alias_ref is None:
                raise ValueError(f"Alias {key} is missing a resolvable upstream task")
            return self._build_expr(_identity, [alias_ref], {})

        if datanode_cls and isinstance(node, datanode_cls):
            return self._build_expr(_identity, [node.value], {})

        if literal_cls and isinstance(node, literal_cls):
            return self._build_expr(_identity, [node.value], {})

        if taskref_cls and isinstance(node, taskref_cls):
            ref = TaskOutputRef(node.key, getattr(node, "path", ()) or ())
            return self._build_expr(_identity, [ref], {})

        if nested_cls and isinstance(node, nested_cls):
            payload = getattr(node, "value", None)
            if payload is None:
                payload = getattr(node, "data", None)
            value = self._unwrap_dts_operand(payload, task_keys, parent_key=key)
            return self._build_expr(_identity, [value], {})

        return self._build_expr(_identity, [node], {})

    def _convert_sexpr_task(self, sexpr, task_keys):
        """Handle legacy sexpr-style nodes by replacing embedded task keys with `TaskOutputRef`."""
        if not isinstance(sexpr, (list, tuple)) or not sexpr:
            raise TypeError(f"Task definition must be a non-empty tuple/list, got {type(sexpr)}")

        func = sexpr[0]
        tail = sexpr[1:]

        if tail and isinstance(tail[-1], Mapping):
            raw_args, raw_kwargs = tail[:-1], tail[-1]
        else:
            raw_args, raw_kwargs = tail, {}

        args = tuple(self._wrap_dependency(arg, task_keys) for arg in raw_args)
        kwargs = {k: self._wrap_dependency(v, task_keys) for k, v in raw_kwargs.items()}

        return func, args, kwargs

    def _wrap_dependency(self, obj, task_keys):
        """Wrap nested objects inside a sexpr when they point at other tasks."""
        if isinstance(obj, TaskOutputRef):
            return obj

        if self._should_wrap(obj, task_keys):
            return TaskOutputRef(obj)

        if isinstance(obj, list):
            return [self._wrap_dependency(v, task_keys) for v in obj]

        if isinstance(obj, tuple):
            if obj and callable(obj[0]):
                head = obj[0]
                tail = tuple(self._wrap_dependency(v, task_keys) for v in obj[1:])
                return (head, *tail)
            return tuple(self._wrap_dependency(v, task_keys) for v in obj)

        if isinstance(obj, Mapping):
            return {k: self._wrap_dependency(v, task_keys) for k, v in obj.items()}

        if isinstance(obj, set):
            return {self._wrap_dependency(v, task_keys) for v in obj}

        if isinstance(obj, frozenset):
            return frozenset(self._wrap_dependency(v, task_keys) for v in obj)

        return obj

    def _should_wrap(self, obj, task_keys):
        """Decide whether a value should become a `TaskOutputRef`."""
        if isinstance(obj, self._LEAF_TYPES):
            if isinstance(obj, str):
                hit = obj in task_keys
                return hit
            return False
        try:
            hit = obj in task_keys
            return hit
        except TypeError:
            return False

    # Flatten Dask collections into the dict-of-tasks structure the rest of the
    # pipeline expects. DAGVine clients often hand us a dict like
    # {"result": dask.delayed(...)}; we merge the underlying HighLevelGraphs so
    # `ContextGraph` sees the same dict representation C does.
    def _dask_collections_to_task_dict(self, task_dict):
        """Flatten Dask collections into the classic dict-of-task layout."""
        assert is_dask_collection is not None
        from dask.highlevelgraph import HighLevelGraph, ensure_dict

        if not isinstance(task_dict, dict):
            raise TypeError("Input must be a dict")

        for k, v in task_dict.items():
            if not is_dask_collection(v):
                raise TypeError(
                    f"Input must be a dict of DaskCollection, but found {k} with type {type(v)}"
                )

        if dts:
            sub_hlgs = [v.dask for v in task_dict.values()]
            hlg = HighLevelGraph.merge(*sub_hlgs).to_dict()
        else:
            hlg = dask.base.collections_to_dsk(task_dict.values())

        return ensure_dict(hlg)

    def _is_dts_node(self, value):
        """Return True when the value is part of the TaskSpec family."""
        if not dts:
            return False
        try:
            return isinstance(value, dts.GraphNode)
        except AttributeError:
            return False

    def _unwrap_dts_operand(self, operand, task_keys, *, parent_key=None):
        """Recursively unwrap TaskSpec operands into pure Python values/containers and `TaskOutputRef`.

        Contract (TaskSpec path only):
        - TaskRef/Alias become `TaskOutputRef` (references, never lifted).
        - Literals/DataNode become plain Python values.
        - NestedContainer unwraps recursively.
        - Task inside args/kwargs is either:
          - treated as a reference when it has a top-level key, or
          - reduced to a pure value only for a small "pure constructor/identity" whitelist, or
          - lifted into a new top-level node and replaced with `TaskOutputRef(new_key)`.
        """
        if not dts:
            return operand

        taskref_cls = getattr(dts, "TaskRef", None)
        if taskref_cls and isinstance(operand, taskref_cls):
            key = getattr(operand, "key", None)
            path = getattr(operand, "path", ())
            return TaskOutputRef(key, path or ())

        alias_cls = getattr(dts, "Alias", None)
        if alias_cls and isinstance(operand, alias_cls):
            alias_ref = self._extract_alias_target(operand, task_keys)
            if alias_ref is None:
                raise ValueError("Alias node is missing a valid upstream source")
            return alias_ref

        literal_cls = getattr(dts, "Literal", None)
        if literal_cls and isinstance(operand, literal_cls):
            value = getattr(operand, "value", None)
            return value

        datanode_cls = getattr(dts, "DataNode", None)
        if datanode_cls and isinstance(operand, datanode_cls):
            value = operand.value
            return value

        nested_cls = getattr(dts, "NestedContainer", None)
        if nested_cls and isinstance(operand, nested_cls):
            payload = getattr(operand, "value", None)
            if payload is None:
                payload = getattr(operand, "data", None)
            value = self._unwrap_dts_operand(payload, task_keys, parent_key=parent_key)
            return value

        task_cls = getattr(dts, "Task", None)
        if task_cls and isinstance(operand, task_cls):
            inline_key = getattr(operand, "key", None)
            # Rule 3: if it is a real graph node (key is present and in task_keys),
            # treat it as a dependency reference.
            if inline_key is not None and inline_key in task_keys:
                return TaskOutputRef(inline_key, ())

            # Otherwise it is an inline expression. Reduce if safe, else lift.
            func = self._extract_callable_from_task(operand)
            if func is None:
                out = self._lift_inline_task(operand, task_keys, parent_key=parent_key)
                return out

            # Special-case: Dask internal identity-cast wrappers should not be called
            # during adaptation. Reduce structurally by unwrapping all args and
            # rebuilding the requested container type. This preserves dependency
            # edges (critical for WCC) without executing arbitrary code.
            if self._is_identity_cast_op(func):
                raw_args = getattr(operand, "args", ()) or ()
                raw_kwargs = getattr(operand, "kwargs", {}) or {}
                typ = raw_kwargs.get("typ", None)

                values = [self._unwrap_dts_operand(a, task_keys, parent_key=parent_key) for a in raw_args]

                # Only allow safe container constructors here; otherwise lift.
                safe_types = (list, tuple, set, frozenset, dict)
                if typ in safe_types:
                    try:
                        casted = typ(values)
                    except Exception:
                        return self._lift_inline_task(operand, task_keys, parent_key=parent_key)
                    return casted

                # Unknown/unsafe typ: lift so the worker executes the real op.
                return self._lift_inline_task(operand, task_keys, parent_key=parent_key)

            if self._is_pure_value_op(func):
                reduced, used_lift = self._reduce_inline_task(operand, task_keys, parent_key=parent_key)
                if used_lift:
                    # Rule 2: if any child required lifting/unknown handling, lift the whole expression.
                    return self._lift_inline_task(operand, task_keys, parent_key=parent_key)
                if self._is_too_large_inline_value(reduced):
                    return self._lift_inline_task(operand, task_keys, parent_key=parent_key)
                return reduced

            # Rule 1: unknown/unsafe op -> must lift.
            return self._lift_inline_task(operand, task_keys, parent_key=parent_key)

        if isinstance(operand, list):
            return [self._unwrap_dts_operand(v, task_keys, parent_key=parent_key) for v in operand]

        if isinstance(operand, tuple):
            return tuple(self._unwrap_dts_operand(v, task_keys, parent_key=parent_key) for v in operand)

        if isinstance(operand, Mapping):
            return {k: self._unwrap_dts_operand(v, task_keys, parent_key=parent_key) for k, v in operand.items()}

        if isinstance(operand, set):
            return {self._unwrap_dts_operand(v, task_keys, parent_key=parent_key) for v in operand}

        if isinstance(operand, frozenset):
            return frozenset(self._unwrap_dts_operand(v, task_keys, parent_key=parent_key) for v in operand)

        return operand

    def _extract_alias_target(self, alias_node, task_keys):
        """Discover which upstream key an alias points at and return it as a `TaskOutputRef`."""
        fields = getattr(alias_node.__class__, "__dataclass_fields__", {}) if dts else {}

        path = getattr(alias_node, "path", ())
        path = tuple(path) if path else ()

        for candidate in ("alias_of", "target", "source", "ref"):
            if candidate in fields:
                raw_value = getattr(alias_node, candidate, None)
                if self._should_wrap(raw_value, task_keys):
                    return TaskOutputRef(raw_value, path)

        deps = getattr(alias_node, "dependencies", None)
        if deps:
            deps = list(deps)
            if len(deps) == 1:
                return TaskOutputRef(deps[0], path)

        return None

    @staticmethod
    def _build_expr(func, args, kwargs):
        return func, tuple(args), dict(kwargs)

    @staticmethod
    def _safe_repr(value, limit=800):
        """Best-effort repr that won't explode logs on huge graphs."""
        try:
            text = repr(value)
        except Exception as e:
            text = f"<unreprable {type(value).__name__}: {e}>"
        if limit and len(text) > limit:
            return text[:limit] + "...<truncated>"
        return text

    @staticmethod
    def _is_pure_value_op(func):
        """Return True if `func` is safe to execute during adaptation to build a pure value.

        This is intentionally conservative: only pure constructors/identity-like ops.
        """
        if func in (dict, list, tuple, set, frozenset):
            return True
        return False

    @staticmethod
    def _is_identity_cast_op(func):
        """Detect Dask's private identity-cast op without executing it."""
        name = getattr(func, "__name__", None)
        module = getattr(func, "__module__", None)
        return bool(name == "_identity_cast" and module and module.startswith("dask"))

    def _reduce_inline_task(self, task_node, task_keys, *, parent_key=None):
        """Best-effort reduction of an inline TaskSpec Task into a pure value.

        Returns (value, used_lift) where used_lift indicates a nested operand triggered lifting.
        """
        func = self._extract_callable_from_task(task_node)
        raw_args = getattr(task_node, "args", ()) or ()
        raw_kwargs = getattr(task_node, "kwargs", {}) or {}

        used_lift = False

        # unwrap args/kwargs; if we see a lifted ref, mark used_lift (Rule 2).
        args = []
        for arg in raw_args:
            before = len(self._lifted_nodes)
            args.append(self._unwrap_dts_operand(arg, task_keys, parent_key=parent_key))
            used_lift = used_lift or (len(self._lifted_nodes) != before)

        kwargs = {}
        for k, v in raw_kwargs.items():
            before = len(self._lifted_nodes)
            kwargs[k] = self._unwrap_dts_operand(v, task_keys, parent_key=parent_key)
            used_lift = used_lift or (len(self._lifted_nodes) != before)

        # Pure constructors are safe to execute even if they contain TaskOutputRefs
        # (they just build containers of refs). Anything else is lifted.
        try:
            value = func(*args, **kwargs)
        except Exception:
            # If evaluation fails, prefer lifting over guessing semantics.
            return self._lift_inline_task(task_node, task_keys, parent_key=parent_key), True

        return value, used_lift

    @staticmethod
    def _is_too_large_inline_value(value, *, max_container_len=2000):
        """Heuristic to avoid inlining huge container constructions that would bloat memory."""
        try:
            if isinstance(value, (list, tuple, set, frozenset, dict)):
                return len(value) > max_container_len
        except Exception:
            return False
        return False

    def _lift_inline_task(self, task_node, task_keys, *, parent_key=None):
        """Lift an inline TaskSpec Task into its own node and return a `TaskOutputRef` to it."""
        inline_key = getattr(task_node, "key", None)
        if parent_key is not None and inline_key == parent_key:
            raise ValueError(f"Refusing to lift Task that would self-reference parent key {parent_key!r}")

        sig = self._dts_structural_signature(task_node, task_keys)
        cached = self._lift_cache.get(sig)
        if cached is not None:
            return TaskOutputRef(cached, ())

        import hashlib

        digest = hashlib.sha1(sig.encode("utf-8")).hexdigest()[:16]
        base = f"__lift__{digest}"
        new_key = base
        # Collision handling + avoid clobbering existing user keys.
        while new_key in task_keys or new_key in self._lifted_nodes:
            self._lift_counter += 1
            new_key = f"{base}_{self._lift_counter}"

        self._lift_cache[sig] = new_key
        self._lifted_nodes[new_key] = task_node
        task_keys.add(new_key)
        return TaskOutputRef(new_key, ())

    def _dts_structural_signature(self, obj, task_keys):
        """Best-effort stable signature for deduping lifted inline expressions."""
        # Keep it deterministic and conservative. If we can't make it stable, fall back to repr.
        try:
            taskref_cls = getattr(dts, "TaskRef", None)
            alias_cls = getattr(dts, "Alias", None)
            literal_cls = getattr(dts, "Literal", None)
            datanode_cls = getattr(dts, "DataNode", None)
            nested_cls = getattr(dts, "NestedContainer", None)
            task_cls = getattr(dts, "Task", None)

            if taskref_cls and isinstance(obj, taskref_cls):
                return f"TaskRef({getattr(obj, 'key', None)!r},{tuple(getattr(obj, 'path', ()) or ())!r})"
            if alias_cls and isinstance(obj, alias_cls):
                ref = self._extract_alias_target(obj, task_keys)
                return f"Alias({getattr(ref, 'task_key', None)!r},{getattr(ref, 'path', ())!r})"
            if literal_cls and isinstance(obj, literal_cls):
                return f"Literal({self._safe_repr(getattr(obj, 'value', None))})"
            if datanode_cls and isinstance(obj, datanode_cls):
                return f"DataNode({self._safe_repr(getattr(obj, 'value', None))})"
            if nested_cls and isinstance(obj, nested_cls):
                payload = getattr(obj, "value", None)
                if payload is None:
                    payload = getattr(obj, "data", None)
                return f"Nested({self._dts_structural_signature(payload, task_keys)})"
            if task_cls and isinstance(obj, task_cls):
                key = getattr(obj, "key", None)
                if key is not None and key in task_keys:
                    return f"TaskKey({key!r})"
                func = self._extract_callable_from_task(obj)
                func_id = (getattr(func, "__module__", None), getattr(func, "__qualname__", None), getattr(func, "__name__", None))
                args = getattr(obj, "args", ()) or ()
                kwargs = getattr(obj, "kwargs", {}) or {}
                arg_sigs = ",".join(self._dts_structural_signature(a, task_keys) for a in args)
                kw_sigs = ",".join(f"{k}={self._dts_structural_signature(v, task_keys)}" for k, v in sorted(kwargs.items()))
                return f"TaskInline(func={func_id!r},args=[{arg_sigs}],kwargs=[{kw_sigs}])"

            if isinstance(obj, list):
                return "list(" + ",".join(self._dts_structural_signature(v, task_keys) for v in obj) + ")"
            if isinstance(obj, tuple):
                return "tuple(" + ",".join(self._dts_structural_signature(v, task_keys) for v in obj) + ")"
            if isinstance(obj, dict):
                items = ",".join(
                    f"{self._safe_repr(k)}:{self._dts_structural_signature(v, task_keys)}"
                    for k, v in sorted(obj.items(), key=lambda kv: repr(kv[0]))
                )
                return "dict(" + items + ")"
            if isinstance(obj, (set, frozenset)):
                items = ",".join(sorted(self._dts_structural_signature(v, task_keys) for v in obj))
                return f"{type(obj).__name__}(" + items + ")"

            return f"py({self._safe_repr(obj)})"
        except Exception:
            return f"fallback({self._safe_repr(obj)})"

    @staticmethod
    def _extract_callable_from_task(node):
        candidates = (
            "function",
            "op",
            "callable",
            "func",
            "operation",
            "callable_obj",
        )

        for attr in candidates:
            if not hasattr(node, attr):
                continue
            value = getattr(node, attr)
            if value is None:
                continue
            if callable(value):
                return value
            if hasattr(value, "__call__"):
                return value

        if hasattr(node, "__call__") and callable(node):
            return node

        return None
