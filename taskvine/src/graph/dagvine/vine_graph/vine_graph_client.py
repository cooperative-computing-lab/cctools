# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""High-level client that exposes the C vine graph to Python callers."""

from . import vine_graph_capi


class VineGraphClient:
    """Python-friendly wrapper that hides the raw SWIG API surface."""

    def __init__(self, c_taskvine):
        """Create and own the lifecycle of the backing C vine graph instance."""
        self._c_graph = vine_graph_capi.vine_graph_create(c_taskvine)
        self._key_to_id = {}
        self._id_to_key = {}

    def tune(self, name, value):
        """Forward tuning parameters directly to the C vine graph."""
        vine_graph_capi.vine_graph_tune(self._c_graph, name, value)

    def add_node(self, key, is_target=None):
        """Create a node in the C graph and remember the key↔id mapping."""
        node_id = vine_graph_capi.vine_graph_add_node(self._c_graph)
        self._key_to_id[key] = node_id
        self._id_to_key[node_id] = key
        if is_target is not None and bool(is_target):
            vine_graph_capi.vine_graph_set_target(self._c_graph, node_id)
        return node_id

    def set_target(self, key):
        """Mark an existing node as a target output."""
        node_id = self._key_to_id.get(key)
        if node_id is None:
            raise KeyError(f"Key not found: {key}")
        vine_graph_capi.vine_graph_set_target(self._c_graph, node_id)

    def add_dependency(self, parent_key, child_key):
        """Add an edge in the C graph using the remembered id mapping."""
        if parent_key not in self._key_to_id or child_key not in self._key_to_id:
            raise KeyError("parent_key or child_key missing in mapping; call add_node() first")
        vine_graph_capi.vine_graph_add_dependency(
            self._c_graph, self._key_to_id[parent_key], self._key_to_id[child_key]
        )

    def compute_topology_metrics(self):
        """Trigger the C graph to compute depth/height, heavy-score, etc."""
        vine_graph_capi.vine_graph_compute_topology_metrics(self._c_graph)

    def get_node_outfile_remote_name(self, key):
        """Ask the C layer where a node's output will be stored."""
        if key not in self._key_to_id:
            raise KeyError(f"Key not found: {key}")
        return vine_graph_capi.vine_graph_get_node_outfile_remote_name(
            self._c_graph, self._key_to_id[key]
        )

    def get_proxy_library_name(self):
        """Expose the randomly generated proxy library name from the C side."""
        return vine_graph_capi.vine_graph_get_proxy_library_name(self._c_graph)

    def set_proxy_function(self, proxy_function):
        """Tell the C graph which Python function should run on the workers."""
        vine_graph_capi.vine_graph_set_proxy_function_name(
            self._c_graph, proxy_function.__name__
        )

    def add_task_input(self, task_key, filename):
        """Add an input file to a task."""
        task_id = self._key_to_id.get(task_key)
        if task_id is None:
            raise KeyError(f"Task key not found: {task_key}")
        vine_graph_capi.vine_graph_add_task_input(self._c_graph, task_id, filename)

    def add_task_output(self, task_key, filename):
        """Add an output file to a task."""
        task_id = self._key_to_id.get(task_key)
        if task_id is None:
            raise KeyError(f"Task key not found: {task_key}")
        vine_graph_capi.vine_graph_add_task_output(self._c_graph, task_id, filename)

    def execute(self):
        """Kick off execution; runs through SWIG down into the C orchestration loop."""
        vine_graph_capi.vine_graph_execute(self._c_graph)

    def get_makespan_us(self):
        """Get the makespan of the vine graph in microseconds."""
        return vine_graph_capi.vine_graph_get_makespan_us(self._c_graph)

    def delete(self):
        """Release the C resources and clear the client."""
        vine_graph_capi.vine_graph_delete(self._c_graph)
