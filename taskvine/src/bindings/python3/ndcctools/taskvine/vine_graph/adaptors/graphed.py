import contextlib
from collections import OrderedDict

from ..workflow import TaskOutputRef, Workflow


class _VineGraphGraphedResources:
    """Small WorkerResources-compatible cache for one VineGraph task."""

    def __init__(self, max_open=128):
        self._handles = OrderedDict()
        self._max_open = max_open

    def open_once(self, uri, opener):
        if uri in self._handles:
            self._handles.move_to_end(uri)
            return self._handles[uri]
        handle = opener(uri)
        self._handles[uri] = handle
        while len(self._handles) > self._max_open:
            _, evicted = self._handles.popitem(last=False)
            self._close_handle(evicted)
        return handle

    def close(self):
        for handle in self._handles.values():
            self._close_handle(handle)
        self._handles.clear()

    @staticmethod
    def _close_handle(handle):
        close = getattr(handle, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()


def _graphed_empty(empty_ref):
    """Run a graphed Plan empty function."""
    empty = empty_ref[0]
    return empty()


def _graphed_process(process_ref, partition):
    """Run one graphed Plan process task."""
    process = process_ref[0]
    resources = _VineGraphGraphedResources()
    try:
        return process(partition, resources)
    finally:
        resources.close()


def _graphed_combine(combine_ref, left, right):
    """Run one graphed Plan combine step."""
    combine = combine_ref[0]
    return combine(left, right)


def _validate_static_plan(plan):
    for attr in ("process", "combine", "empty", "tasks"):
        if not hasattr(plan, attr):
            raise TypeError(f"graphed plan is missing required attribute {attr!r}")
    if getattr(plan, "next_tasks", None) is not None:
        raise ValueError("VineGraphGraphedAdaptor only supports static graphed plans")
    if getattr(plan, "stop", None) is not None:
        raise ValueError("VineGraphGraphedAdaptor does not support graphed StopCondition yet")


def graphed_plan_to_workflow(plan, key_prefix="graphed"):
    """Convert a static graphed Plan into a VineGraph Workflow."""
    _validate_static_plan(plan)

    workflow = Workflow()
    tasks = sorted(tuple(plan.tasks), key=lambda task: task.key)

    empty_key = (key_prefix, "empty")
    workflow.add_task(empty_key, _graphed_empty, [plan.empty])

    previous_key = empty_key
    for task in tasks:
        task_key = (key_prefix, "task", task.key)
        combine_key = (key_prefix, "combine", task.key)

        workflow.add_task(task_key, _graphed_process, [plan.process], task.partition)
        workflow.add_task(
            combine_key,
            _graphed_combine,
            [plan.combine],
            TaskOutputRef(previous_key),
            TaskOutputRef(task_key),
        )
        previous_key = combine_key

    workflow.finalize()
    return workflow, previous_key


class VineGraphGraphedAdaptor:
    """Convert a static graphed Plan into a VineGraph Workflow."""

    def __init__(self, plan, key_prefix="graphed"):
        self.plan = plan
        self.converted, self.target_key = graphed_plan_to_workflow(plan, key_prefix=key_prefix)
        self.target_keys = [self.target_key]

    @property
    def task_dict(self):
        return self.converted
