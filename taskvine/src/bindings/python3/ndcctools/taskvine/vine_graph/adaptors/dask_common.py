"""Shared helpers for Dask-to-VineGraph adaptor modules."""

from ..workflow import TaskOutputRef


def identity(value):
    """Return ``value`` unchanged."""
    return value


def build_task_expr(func, args, kwargs):
    """Build the normalized ``Workflow`` task-expression tuple."""
    return func, tuple(args), dict(kwargs)


def resolve_graph_key_if_task(obj, workflow_keys):
    """Return the matching graph key when ``obj`` denotes an existing Dask task."""
    if isinstance(obj, TaskOutputRef):
        return None
    try:
        if obj in workflow_keys:
            return obj
    except TypeError:
        pass
    if hasattr(obj, "item") and callable(obj.item):
        try:
            item = obj.item()
            if item in workflow_keys:
                return item
        except Exception:
            pass
    return None
