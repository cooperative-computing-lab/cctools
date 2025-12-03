# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager

from ndcctools.taskvine.dagvine.context_graph.proxy_library import ProxyLibrary
from ndcctools.taskvine.dagvine.context_graph.proxy_functions import compute_single_key
from ndcctools.taskvine.dagvine.context_graph.core import ContextGraph, ContextGraphTaskResult
from ndcctools.taskvine.dagvine.vine_graph.vine_graph_client import VineGraphClient

import cloudpickle
import os
import signal
import json

try:
    import dask
except ImportError:
    dask = None

try:
    from dask.base import is_dask_collection
except ImportError:
    is_dask_collection = None

try:
    import dask._task_spec as dts
except ImportError:
    dts = None


def delete_all_files(root_dir):
    """Clean the run-info template directory between runs so stale files never leak into a new DAG."""
    if not os.path.exists(root_dir):
        return
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                print(f"Failed to delete file {file_path}")


# Nicely format terminal output when printing manager metadata.
def color_text(text, color_code):
    """Render a colored string for the friendly status banners Vineyard prints at start-up."""
    return f"\033[{color_code}m{text}\033[0m"


# Flatten Dask collections into the dict-of-tasks structure the rest of the
# pipeline expects. DAGVine clients often hand us a dict like
# {"result": dask.delayed(...)}; we merge the underlying HighLevelGraphs so
# `ContextGraph` sees the same dict representation C does.
def dask_collections_to_task_dict(collection_dict):
    """Merge user-facing Dask collections into the flattened task dict the ContextGraph expects."""
    assert is_dask_collection is not None
    from dask.highlevelgraph import HighLevelGraph, ensure_dict

    if not isinstance(collection_dict, dict):
        raise TypeError("Input must be a dict")

    for k, v in collection_dict.items():
        if not is_dask_collection(v):
            raise TypeError(f"Input must be a dict of DaskCollection, but found {k} with type {type(v)}")

    if dts:
        # the new Dask API
        sub_hlgs = [v.dask for v in collection_dict.values()]
        hlg = HighLevelGraph.merge(*sub_hlgs).to_dict()
    else:
        # the old Dask API
        hlg = dask.base.collections_to_dsk(collection_dict.values())

    return ensure_dict(hlg)


# Accept both plain dicts and Dask collections from callers. Most library users
# hand us `{key: delayed / value}` directly, while some experiments pass a
# fully-expanded legacy Dask dict. This helper normalises both cases so the rest
# of the pipeline only deals with `{task_key: task_expression}`.
def ensure_task_dict(collection_dict):
    """Normalize user input (raw dict or Dask collection) into a plain `{task_key: expr}` mapping."""
    if is_dask_collection and any(is_dask_collection(v) for v in collection_dict.values()):
        task_dict = dask_collections_to_task_dict(collection_dict)
    else:
        task_dict = collection_dict

    if dts:
        return dts.convert_legacy_graph(task_dict)
    else:
        return task_dict


class GraphParams:
    def __init__(self):
        """Hold all tweakable knobs (manager-side, vine_graph-side, and misc)."""
        # Manager-level knobs: fed into `Manager.tune(...)` before execution.
        self.vine_manager_tuning_params = {
            "worker-source-max-transfers": 100,
            "max-retrievals": -1,
            "prefer-dispatch": 1,
            "transient-error-interval": 1,
            "attempt-schedule-depth": 10000,
            "temp-replica-count": 1,
            "enforce-worker-eviction-interval": -1,
            "balance-worker-disk-load": 0,
        }
        # VineGraph-level knobs: forwarded to the underlying vine graph via VineGraphClient.
        self.vine_graph_tuning_params = {
            "failure-injection-step-percent": -1,
            "task-priority-mode": "largest-input-first",
            "prune-depth": 1,
            "output-dir": "./outputs",
            "checkpoint-dir": "./checkpoints",
            "checkpoint-fraction": 0,
            "progress-bar-update-interval-sec": 0.1,
            "time-metrics-filename": "time_metrics.csv",
            "enable-debug-log": 1,
        }
        # Misc knobs used purely on the Python side (e.g., generate fake outputs).
        self.other_params = {
            "schedule": "worst",
            "libcores": 16,
            "failure-injection-step-percent": -1,
            "extra-task-output-size-mb": [0, 0],
            "extra-task-sleep-time": [0, 0],
        }

    def print_params(self):
        """Dump current knob values to stdout for debugging."""
        all_params = {**self.vine_manager_tuning_params, **self.vine_graph_tuning_params, **self.other_params}
        print(json.dumps(all_params, indent=4))

    def update_param(self, param_name, new_value):
        """Update a single knob, falling back to manager-level if unknown."""
        if param_name in self.vine_manager_tuning_params:
            self.vine_manager_tuning_params[param_name] = new_value
        elif param_name in self.vine_graph_tuning_params:
            self.vine_graph_tuning_params[param_name] = new_value
        elif param_name in self.other_params:
            self.other_params[param_name] = new_value
        else:
            self.vine_manager_tuning_params[param_name] = new_value

    def get_value_of(self, param_name):
        """Helper so DAGVine can pull a knob value without caring where it lives."""
        if param_name in self.vine_manager_tuning_params:
            return self.vine_manager_tuning_params[param_name]
        elif param_name in self.vine_graph_tuning_params:
            return self.vine_graph_tuning_params[param_name]
        elif param_name in self.other_params:
            return self.other_params[param_name]
        else:
            raise ValueError(f"Invalid param name: {param_name}")


class DAGVine(Manager):
    def __init__(self,
                 *args,
                 **kwargs):
        """Spin up a TaskVine manager that knows how to mirror a Python DAG into the C orchestration layer."""

        # React to Ctrl+C so we can tear down the graphs cleanly.
        signal.signal(signal.SIGINT, self._on_sigint)

        self.params = GraphParams()

        # Ensure run-info templates don't accumulate garbage between runs.
        run_info_path = kwargs.get("run_info_path", None)
        run_info_template = kwargs.get("run_info_template", None)
        self.run_info_template_path = os.path.join(run_info_path, run_info_template)
        if self.run_info_template_path:
            delete_all_files(self.run_info_template_path)

        # Boot the underlying TaskVine manager. The TaskVine manager keeps alive until the dagvine object is destroyed
        super().__init__(*args, **kwargs)
        print(f"cvine = {cvine}")
        self.runtime_directory = cvine.vine_get_runtime_directory(self._taskvine)

        print(f"=== Manager name: {color_text(self.name, 92)}")
        print(f"=== Manager port: {color_text(self.port, 92)}")
        print(f"=== Runtime directory: {color_text(self.runtime_directory, 92)}")

    def param(self, param_name):
        """Convenience accessor so callers can read tuned parameters at runtime."""
        return self.params.get_value_of(param_name)

    def update_params(self, new_params):
        """Apply a batch of overrides before constructing graphs.

        All parameter dictionaries—whether set via `update_params()` or passed
        to `run(..., params={...})`—flow through here. We funnel each key into
        the appropriate bucket (manager/vine_graph/misc). Subsequent runs can override
        them by calling this again.
        """
        assert isinstance(new_params, dict), "new_params must be a dict"
        for k, new_v in new_params.items():
            self.params.update_param(k, new_v)

    def tune_manager(self):
        """Push our manager-side tuning knobs into the C layer."""
        for k, v in self.params.vine_manager_tuning_params.items():
            try:
                self.tune(k, v)
            except Exception:
                raise ValueError(f"Unrecognized parameter: {k}")

    def tune_vine_graph(self, vine_graph):
        """Push VineGraph-specific tuning knobs before we build the graph."""
        for k, v in self.params.vine_graph_tuning_params.items():
            vine_graph.tune(k, str(v))

    def build_context_graph(self, task_dict):
        """Construct the Python-side DAG wrapper (ContextGraph)."""
        context_graph = ContextGraph(
            task_dict,
            extra_task_output_size_mb=self.param("extra-task-output-size-mb"),
            extra_task_sleep_time=self.param("extra-task-sleep-time")
        )

        return context_graph

    def build_vine_graph(self, context_graph, target_keys):
        """Mirror the ContextGraph into VineGraph, preserving ordering and targets."""
        assert context_graph is not None, "ContextGraph must be built before building the VineGraph"

        vine_graph = VineGraphClient(self._taskvine)

        vine_graph.set_proxy_function(compute_single_key)

        # Tune both manager and vine_graph before we start adding nodes/edges.
        self.tune_manager()
        self.tune_vine_graph(vine_graph)

        topo_order = context_graph.get_topological_order()
        # Build the cross-language mapping as we walk the topo order.
        for k in topo_order:
            node_id = vine_graph.add_node(k)
            context_graph.ckey2vid[k] = node_id
            context_graph.vid2ckey[node_id] = k
            for pk in context_graph.parents_of[k]:
                vine_graph.add_dependency(pk, k)

        # Now that every node is present, mark which ones are final outputs.
        for k in target_keys:
            vine_graph.set_target(k)

        vine_graph.compute_topology_metrics()

        return vine_graph

    def build_graphs(self, task_dict, target_keys):
        """Create both the ContextGraph and its C counterpart, wiring outputs for later use."""
        # Build the logical (Python) DAG.
        context_graph = self.build_context_graph(task_dict)
        # Build the physical (C) DAG.
        vine_graph = self.build_vine_graph(context_graph, target_keys)

        # Cross-fill the outfile locations so the runtime graph knows where to read/write.
        for k in context_graph.ckey2vid:
            outfile_remote_name = vine_graph.get_node_outfile_remote_name(k)
            context_graph.outfile_remote_name[k] = outfile_remote_name

        return context_graph, vine_graph

    def create_proxy_library(self, context_graph, vine_graph, hoisting_modules, env_files):
        """Package up the context_graph as a TaskVine library."""
        proxy_library = ProxyLibrary(self)
        proxy_library.add_hoisting_modules(hoisting_modules)
        proxy_library.add_env_files(env_files)
        proxy_library.set_context_loader(ContextGraph.context_loader_func, context_loader_args=[cloudpickle.dumps(context_graph)])
        proxy_library.set_libcores(self.param("libcores"))
        proxy_library.set_name(vine_graph.get_proxy_library_name())

        return proxy_library

    def run(self, collection_dict, target_keys=[], params={}, hoisting_modules=[], env_files={}):
        """High-level entry point: normalise input, build graphs, ship the library, execute, and return results."""
        # first update the params so that they can be used for the following construction
        self.update_params(params)

        # filter out target keys that are not in the collection dict
        missing_keys = [k for k in target_keys if k not in collection_dict]
        if missing_keys:
            print(f"=== Warning: the following target keys are not in the collection dict:")
            for k in missing_keys:
                print(f"             {k}")
        target_keys = list(set(target_keys) - set(missing_keys))

        task_dict = ensure_task_dict(collection_dict)

        # Build both the Python DAG and its C mirror.
        context_graph, vine_graph = self.build_graphs(task_dict, target_keys)

        # Ship the execution context to workers via a proxy library.
        proxy_library = self.create_proxy_library(context_graph, vine_graph, hoisting_modules, env_files)
        proxy_library.install()

        print(f"=== Library serialized size: {color_text(proxy_library.get_context_size(), 92)} MB")

        # Kick off execution on the C side.
        vine_graph.execute()

        # Tear down once we're done so successive runs start clean.
        proxy_library.uninstall()

        # Delete the C graph immediately so its lifetime matches the run.
        vine_graph.delete()

        # Load any requested target outputs back into Python land.
        results = {}
        for k in target_keys:
            outfile_path = os.path.join(self.param("output-dir"), context_graph.outfile_remote_name[k])
            results[k] = ContextGraphTaskResult.load_from_path(outfile_path)
        return results

    def _on_sigint(self, signum, frame):
        """SIGINT handler that delegates to Manager cleanup so workers are released promptly."""
        self.__del__()
