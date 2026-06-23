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
    dts = None

from .legacy_dask_adaptor import expand_legacy_subgraph_dsk
from .dask_common import (
    build_task_expr,
    identity,
    resolve_graph_key_if_task,
)
from .dask_task_spec import DaskTaskSpecConverter
from ..workflow import TaskOutputRef, Workflow


def _apply_with_kwargs_kvlist(func, args_list, kwargs_kvlist):
    """Call ``func`` when kwargs are encoded as ``[[key, value], ...]``."""
    return func(*args_list, **{key: value for key, value in kwargs_kvlist})


def workflow_to_dask_graph(workflow):
    assert isinstance(workflow, Workflow), "workflow must be a Workflow"

    def ref_to_key(ref):
        return ref.workflow_key

    dsk = {}
    for workflow_key, (func_id, args, kwargs) in workflow.task_dict.items():
        func = workflow.callables[func_id]
        new_args = workflow._visit_task_output_refs(args, ref_to_key, rewrite=True)
        new_kwargs = workflow._visit_task_output_refs(kwargs, ref_to_key, rewrite=True)

        if new_kwargs:
            dsk[workflow_key] = (_apply_with_kwargs_kvlist, func, list(new_args), [[k, v] for k, v in new_kwargs.items()])
        else:
            dsk[workflow_key] = (func, *new_args)

    return dsk


class VineGraphDaskAdaptor:
    """Convert Dask graph forms into VineGraph ``Workflow`` task expressions."""

    def __init__(self, task_dict, expand_subgraphs=False):
        if isinstance(task_dict, Workflow):
            self.converted = task_dict
            return

        self._expand_subgraphs = expand_subgraphs
        normalized = self._normalize_task_dict(task_dict)
        self.converted = self._convert_to_workflow_tasks(normalized)

    @property
    def task_dict(self):
        return self.converted

    def _normalize_task_dict(self, task_dict):
        if self._is_dask_collection_dict(task_dict):
            task_dict = self._dask_collections_to_task_dict(task_dict)
        else:
            # Plain dicts are VineGraph sexprs; don't let Dask reinterpret kwargs dicts.
            task_dict = dict(task_dict)

        if self._expand_subgraphs and not dts and task_dict:
            task_dict = expand_legacy_subgraph_dsk(task_dict, dask)
        return task_dict

    def _is_dask_collection_dict(self, task_dict):
        return bool(is_dask_collection and any(is_dask_collection(value) for value in task_dict.values()))

    def _dask_collections_to_task_dict(self, task_dict):
        assert is_dask_collection is not None
        from dask.highlevelgraph import HighLevelGraph, ensure_dict

        if not isinstance(task_dict, dict):
            raise TypeError("Input must be a dict")
        for key, value in task_dict.items():
            if not is_dask_collection(value):
                raise TypeError(f"Input must be a dict of DaskCollection, but found {key} with type {type(value)}")

        if dts:
            hlg = HighLevelGraph.merge(*(value.dask for value in task_dict.values())).to_dict()
        else:
            hlg = dask.base.collections_to_dsk(task_dict.values())
            hlg = hlg.to_dict() if hasattr(hlg, "to_dict") else dict(hlg)
        return ensure_dict(hlg)

    def _convert_to_workflow_tasks(self, task_dict):
        if not task_dict:
            return {}

        converted = {}
        workflow_keys = set(task_dict.keys())
        task_spec = DaskTaskSpecConverter(dts) if dts else None

        for key, value in task_dict.items():
            if task_spec and task_spec.is_node(value):
                converted[key] = task_spec.convert_node(key, value, workflow_keys)
            else:
                converted[key] = self._convert_legacy_task(value, workflow_keys)

        if task_spec:
            while True:
                pending = task_spec.pending_nodes(converted)
                if not pending:
                    break
                for key, node in pending:
                    converted[key] = task_spec.convert_node(key, node, workflow_keys)

        return converted

    def _convert_legacy_task(self, sexpr, workflow_keys):
        try:
            if not isinstance(sexpr, (list, tuple)) and sexpr in workflow_keys:
                return build_task_expr(identity, [TaskOutputRef(sexpr)], {})
        except TypeError:
            pass

        if not isinstance(sexpr, (list, tuple)):
            return build_task_expr(identity, [sexpr], {})
        if not sexpr:
            raise TypeError("Task definition must be a non-empty tuple/list")

        func = sexpr[0]
        tail = sexpr[1:]
        if tail and isinstance(tail[-1], Mapping):
            raw_args, raw_kwargs = tail[:-1], tail[-1]
        else:
            raw_args, raw_kwargs = tail, {}

        args = tuple(self._wrap_dependency(arg, workflow_keys) for arg in raw_args)
        kwargs = {key: self._wrap_dependency(value, workflow_keys) for key, value in raw_kwargs.items()}
        return func, args, kwargs

    def _wrap_dependency(self, obj, workflow_keys):
        if isinstance(obj, TaskOutputRef):
            return obj

        key = resolve_graph_key_if_task(obj, workflow_keys)
        if key is not None:
            return TaskOutputRef(key)

        if isinstance(obj, list):
            return [self._wrap_dependency(value, workflow_keys) for value in obj]
        if isinstance(obj, tuple):
            return tuple(self._wrap_dependency(value, workflow_keys) for value in obj)
        if isinstance(obj, Mapping):
            return {key: self._wrap_dependency(value, workflow_keys) for key, value in obj.items()}
        if isinstance(obj, set):
            return {self._wrap_dependency(value, workflow_keys) for value in obj}
        if isinstance(obj, frozenset):
            return frozenset(self._wrap_dependency(value, workflow_keys) for value in obj)
        return obj
