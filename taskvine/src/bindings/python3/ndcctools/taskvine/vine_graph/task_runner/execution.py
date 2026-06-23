# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import sys
import time
from collections import defaultdict, deque

from ndcctools.taskvine.utils import load_variable_from_library


def _resolve_nested_legacy_tasks(obj):
    """Evaluate nested legacy Dask tasks encoded as plain ``(func, *args)`` tuples."""
    if type(obj) is tuple and obj and callable(obj[0]):
        func = obj[0]
        args = tuple(_resolve_nested_legacy_tasks(v) for v in obj[1:])
        return func(*args)

    if isinstance(obj, list):
        return [_resolve_nested_legacy_tasks(v) for v in obj]

    if type(obj) is tuple:
        return tuple(_resolve_nested_legacy_tasks(v) for v in obj)

    if isinstance(obj, dict):
        return {k: _resolve_nested_legacy_tasks(v) for k, v in obj.items()}

    if isinstance(obj, set):
        return {_resolve_nested_legacy_tasks(v) for v in obj}

    if isinstance(obj, frozenset):
        return frozenset(_resolve_nested_legacy_tasks(v) for v in obj)

    return obj


def compute_task(workflow, task_expr):
    func_id, args, kwargs = task_expr
    func = workflow.callables[func_id]
    cache = {}

    def _follow_path(value, path):
        current = value
        for token in path:
            if isinstance(current, (list, tuple)):
                current = current[token]
            elif isinstance(current, dict):
                current = current[token]
            else:
                current = getattr(current, token)
        return current

    def on_ref(r):
        x = cache.get(r.workflow_key)
        if x is None:
            x = workflow.load_task_output(r.workflow_key)
            cache[r.workflow_key] = x
        if r.path:
            return _follow_path(x, r.path)
        return x

    r_args = workflow._visit_task_output_refs(args, on_ref, rewrite=True)
    r_kwargs = workflow._visit_task_output_refs(kwargs, on_ref, rewrite=True)

    r_args = _resolve_nested_legacy_tasks(r_args)
    r_kwargs = _resolve_nested_legacy_tasks(r_kwargs)

    return func(*r_args, **r_kwargs)


def topo_sort_group_scheduler_keys(workflow, member_scheduler_keys):
    """Return a topological order of scheduler keys for one batched task-runner call.

    Edges count only when both endpoints lie in the batch. Ties are broken by the order keys
    appear in the argument list which usually matches the CSV written by C with the leader first.
    """
    # Kahn tie-break uses the order keys were listed because plain sets forget that order.
    ordered = []
    seen = set()
    for k in member_scheduler_keys:
        kk = int(k)
        if kk not in seen:
            seen.add(kk)
            ordered.append(kk)

    nodes = set(ordered)
    order_index = {k: i for i, k in enumerate(ordered)}
    adj = defaultdict(set)
    indeg = {k: 0 for k in nodes}

    # Restrict Workflow to this batch: edge parent_sk -> child_sk only when both endpoints are members.
    # Aligns with the C executor DAG via the same parents_of as Python Workflow.
    for child_sk in nodes:
        wk_child = workflow.scheduler_key_to_workflow_key[child_sk]
        for wk_parent in workflow.parents_of.get(wk_child, ()):
            parent_sk = workflow.workflow_key_to_scheduler_key[wk_parent]
            if parent_sk in nodes and child_sk not in adj[parent_sk]:
                adj[parent_sk].add(child_sk)
                indeg[child_sk] += 1

    # Kahn: serial execution order within this task-runner call.
    q = deque(k for k in ordered if indeg[k] == 0)
    out = []
    while q:
        u = q.popleft()
        out.append(u)
        ready_next = []
        for v in adj[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                ready_next.append(v)
        ready_next.sort(key=lambda x: order_index[x])
        for v in ready_next:
            q.append(v)
    if len(out) != len(nodes):
        raise ValueError("task_group: cycle among members per Workflow graph")
    return out


def run_single_workflow_node(workflow, scheduler_key):
    """Run one node: scheduler_key -> workflow_key, execute, write outfile for downstream refs."""
    workflow_key = workflow.scheduler_key_to_workflow_key[scheduler_key]
    task_expr = workflow.task_dict[workflow_key]

    output = compute_task(workflow, task_expr)

    time.sleep(workflow.extra_task_sleep_time[workflow_key])

    workflow.save_task_output(workflow_key, output)


def _scheduler_keys_spec_to_list(scheduler_keys_spec):
    """
    Normalize task-runner input payload to a list of integer scheduler keys.
    Primary wire form is one comma-separated string (e.g. ``\"1,2,3\"``).
    Also accepts a bare int (legacy JSON) or a list of ints/strings from json.
    """
    if isinstance(scheduler_keys_spec, str):
        parts = [p.strip() for p in scheduler_keys_spec.split(",") if p.strip()]
        if not parts:
            raise ValueError("run_scheduler_keys: empty scheduler key list after parsing")
        return [int(p, 10) for p in parts]
    if isinstance(scheduler_keys_spec, int):
        return [scheduler_keys_spec]
    if isinstance(scheduler_keys_spec, list):
        if not scheduler_keys_spec:
            raise ValueError("run_scheduler_keys: empty list")
        return [int(x) for x in scheduler_keys_spec]
    raise TypeError(
        f"run_scheduler_keys: expected str, int, or list of keys, got {type(scheduler_keys_spec).__name__}"
    )


def _workflow_from_task_runner_context():
    """
    Resolve the Workflow from the TaskVine function context.

    The normal path is ``ndcctools.taskvine.utils.load_variable_from_library``.
    Keep a ``__main__`` lookup first so generated function scripts that inject
    context directly into their own module namespace also work.
    """
    main = sys.modules.get("__main__")
    g = getattr(main, "graph", None) if main is not None else None
    if g is not None:
        return g
    return load_variable_from_library("graph")


def run_scheduler_keys(scheduler_keys_spec):
    """Task runner entry that parses keys, orders them, runs each node, and writes outfiles."""
    workflow = _workflow_from_task_runner_context()
    keys = _scheduler_keys_spec_to_list(scheduler_keys_spec)
    ordered = topo_sort_group_scheduler_keys(workflow, keys)
    leader_sk = keys[0]
    # The infile from C lists the leader first. Kahn might pick another indeg-zero key first which
    # would reorder writes and confuse executor validation, so move the leader to the front
    # when it has no parent that is also inside this batch.
    if ordered[0] != leader_sk:
        batch = set(ordered)
        wk_leader = workflow.scheduler_key_to_workflow_key[leader_sk]
        for wkp in workflow.parents_of.get(wk_leader, ()):
            psk = workflow.workflow_key_to_scheduler_key[wkp]
            if psk in batch:
                raise ValueError(
                    f"run_scheduler_keys: leader {leader_sk} must run first but has intra-batch parent {psk}"
                )
        ordered = [leader_sk] + [sk for sk in ordered if sk != leader_sk]

    for sk in ordered:
        run_single_workflow_node(workflow, sk)
