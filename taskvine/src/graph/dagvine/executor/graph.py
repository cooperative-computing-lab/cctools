# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Python wrapper for the C executor graph API."""

import json

from . import graph_capi


def format_scheduler_keys_runner_payload(scheduler_keys):
    """
    JSON string for the task-runner ``infile`` buffer: comma-separated scheduler keys in ``fn_args[0]``.
    Compatible with worker ``task_runner.task.run_scheduler_keys`` (parses string / list / int).
    Workflow keys ↔ scheduler keys mapping stays on the executor graph (``ExecutorGraph.add_node``).

    Use this helper when assembling a multi-key call from Python.
    """
    keys_list = scheduler_keys if isinstance(scheduler_keys, (list, tuple)) else [scheduler_keys]
    if not keys_list:
        raise ValueError("scheduler_keys must be non-empty")
    joined = ",".join(str(int(k)) for k in keys_list)
    return json.dumps({"fn_args": [joined], "fn_kwargs": {}})


class ExecutorGraph:
    """Thin wrapper around the SWIG bindings."""

    def __init__(self, c_taskvine):
        """Create the backing C executor graph."""
        self._c_graph = graph_capi.executor_graph_create(c_taskvine)
        self._c_executor = graph_capi.executor_create(c_taskvine, self._c_graph)
        self._workflow_key_to_scheduler_key = {}
        self._scheduler_key_to_workflow_key = {}

    def tune(self, name, value):
        """Forward a tuning parameter to the C executor."""
        if graph_capi.executor_tune(self._c_executor, name, value) != 0:
            raise RuntimeError(f"Failed to tune executor parameter {name!r}={value!r}")

    def add_node(self, workflow_key, is_target=None):
        """Create a C node and record its workflow key."""
        node_id = graph_capi.executor_add_node(self._c_executor)
        self._workflow_key_to_scheduler_key[workflow_key] = node_id
        self._scheduler_key_to_workflow_key[node_id] = workflow_key
        if is_target is not None and bool(is_target):
            graph_capi.graph_set_target(self._c_graph, node_id)
        return node_id

    def set_target(self, workflow_key):
        """Mark a node as a target."""
        node_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if node_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        graph_capi.graph_set_target(self._c_graph, node_id)

    def add_dependency(self, parent_workflow_key, child_workflow_key):
        """Add an edge between two existing nodes."""
        wk2sk = self._workflow_key_to_scheduler_key
        if parent_workflow_key not in wk2sk or child_workflow_key not in wk2sk:
            raise KeyError("parent or child workflow_key missing in mapping; call add_node() first")
        graph_capi.graph_add_dependency(
            self._c_graph, wk2sk[parent_workflow_key], wk2sk[child_workflow_key]
        )

    def group_chain_like_tasks(self):
        """Merge maximal singleton linear chains into supernodes (C graph_group_chain_like_tasks)."""
        return graph_capi.graph_group_chain_like_tasks(self._c_graph)

    def compute_topology_metrics(self):
        """Finalize the C graph and compute topology metrics."""
        graph_capi.executor_finalize(self._c_executor)

    def get_node_outfile_remote_name(self, workflow_key):
        """Return the output path assigned by the C graph."""
        if workflow_key not in self._workflow_key_to_scheduler_key:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        return graph_capi.graph_get_node_outfile_remote_name(
            self._c_graph, self._workflow_key_to_scheduler_key[workflow_key]
        )

    def get_task_runner_library_name(self):
        """Return the generated task runner library name."""
        return graph_capi.graph_get_task_runner_library_name(self._c_graph)

    def set_task_runner_function(self, task_runner_function):
        """Set the worker-side task runner entry point."""
        graph_capi.graph_set_task_runner_function_name(
            self._c_graph, task_runner_function.__name__
        )

    def add_task_input(self, workflow_key, filename):
        """Add an input file to a task."""
        task_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if task_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        graph_capi.executor_add_task_input(self._c_executor, task_id, filename)

    def add_task_output(self, workflow_key, filename):
        """Add an output file to a task."""
        task_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if task_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        graph_capi.executor_add_task_output(self._c_executor, task_id, filename)

    def execute(self):
        """Execute the graph."""
        graph_capi.executor_execute(self._c_executor)

    def get_makespan_us(self):
        """Return the graph makespan in microseconds."""
        return graph_capi.executor_get_makespan_us(self._c_executor)

    def get_total_recovery_tasks(self):
        """Return the total number of submitted recovery tasks."""
        return graph_capi.executor_get_total_recovery_tasks(self._c_executor)

    def get_completed_recovery_tasks(self):
        """Return the number of completed recovery tasks."""
        return graph_capi.executor_get_completed_recovery_tasks(self._c_executor)

    def delete(self):
        """Delete the backing C graph."""
        graph_capi.executor_delete(self._c_executor)
        self._c_executor = None
        graph_capi.graph_delete(self._c_graph)
        self._c_graph = None
