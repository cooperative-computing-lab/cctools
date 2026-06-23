# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Bridge from Python VineGraph objects to the C vine_graph API."""

import importlib
import sys

# SWIG-generated vine_graph_capi.py imports "cvine"; wire that top-level name
# to the real TaskVine Python module before importing the generated bindings.
sys.modules.setdefault("cvine", importlib.import_module("ndcctools.taskvine.cvine"))

from . import vine_graph_capi


class VineGraphCapiBridge:
    """Thin bridge around the SWIG bindings."""

    def __init__(self, c_taskvine):
        """Create the backing C vine_graph objects."""
        self._c_graph = vine_graph_capi.vine_graph_executor_create_graph(c_taskvine)
        self._c_executor = vine_graph_capi.vine_graph_executor_create(c_taskvine, self._c_graph)
        self._workflow_key_to_scheduler_key = {}
        self._scheduler_key_to_workflow_key = {}

    def tune(self, name, value):
        """Forward a tuning parameter to the C vine_graph executor."""
        if vine_graph_capi.vine_graph_executor_tune(self._c_executor, name, value) != 0:
            raise RuntimeError(f"Failed to tune executor parameter {name!r}={value!r}")

    def add_node(self, workflow_key, is_target=None):
        """Create a C node and record its workflow key."""
        node_id = vine_graph_capi.vine_graph_executor_add_node(self._c_executor)
        self._workflow_key_to_scheduler_key[workflow_key] = node_id
        self._scheduler_key_to_workflow_key[node_id] = workflow_key
        if is_target is not None and bool(is_target):
            vine_graph_capi.vine_graph_set_target(self._c_graph, node_id)
        return node_id

    def set_target(self, workflow_key):
        """Mark a node as a target."""
        node_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if node_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        vine_graph_capi.vine_graph_set_target(self._c_graph, node_id)

    def add_dependency(self, parent_workflow_key, child_workflow_key):
        """Add an edge between two existing nodes."""
        wk2sk = self._workflow_key_to_scheduler_key
        if parent_workflow_key not in wk2sk or child_workflow_key not in wk2sk:
            raise KeyError("parent or child workflow_key missing in mapping; call add_node() first")
        vine_graph_capi.vine_graph_add_dependency(
            self._c_graph, wk2sk[parent_workflow_key], wk2sk[child_workflow_key]
        )

    def group_chain_like_tasks(self):
        """Merge maximal singleton linear chains into supernodes (C vine_graph_group_chain_like_tasks)."""
        return vine_graph_capi.vine_graph_group_chain_like_tasks(self._c_graph)

    def compute_topology_metrics(self):
        """Finalize the C graph and compute topology metrics."""
        vine_graph_capi.vine_graph_executor_finalize(self._c_executor)

    def get_node_outfile_remote_name(self, workflow_key):
        """Return the output path assigned by the C graph."""
        if workflow_key not in self._workflow_key_to_scheduler_key:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        return vine_graph_capi.vine_graph_get_node_outfile_remote_name(
            self._c_graph, self._workflow_key_to_scheduler_key[workflow_key]
        )

    def get_task_runner_library_name(self):
        """Return the generated task runner library name."""
        return vine_graph_capi.vine_graph_get_task_runner_library_name(self._c_graph)

    def set_task_runner_function(self, task_runner_function):
        """Set the worker-side task runner entry point."""
        vine_graph_capi.vine_graph_set_task_runner_function_name(
            self._c_graph, task_runner_function.__name__
        )

    def add_task_input(self, workflow_key, filename):
        """Add an input file to a task."""
        task_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if task_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        vine_graph_capi.vine_graph_executor_add_task_input(self._c_executor, task_id, filename)

    def add_task_output(self, workflow_key, filename):
        """Add an output file to a task."""
        task_id = self._workflow_key_to_scheduler_key.get(workflow_key)
        if task_id is None:
            raise KeyError(f"Workflow key not found: {workflow_key}")
        vine_graph_capi.vine_graph_executor_add_task_output(self._c_executor, task_id, filename)

    def execute(self):
        """Execute the graph."""
        vine_graph_capi.vine_graph_executor_execute(self._c_executor)

    def get_makespan_us(self):
        """Return the graph makespan in microseconds."""
        return vine_graph_capi.vine_graph_executor_get_makespan_us(self._c_executor)

    def get_total_recovery_tasks(self):
        """Return the total number of submitted recovery tasks."""
        return vine_graph_capi.vine_graph_executor_get_total_recovery_tasks(self._c_executor)

    def get_completed_recovery_tasks(self):
        """Return the number of completed recovery tasks."""
        return vine_graph_capi.vine_graph_executor_get_completed_recovery_tasks(self._c_executor)

    def delete(self):
        """Delete the backing C graph."""
        vine_graph_capi.vine_graph_executor_delete(self._c_executor)
        self._c_executor = None
        vine_graph_capi.vine_graph_delete(self._c_graph)
        self._c_graph = None
