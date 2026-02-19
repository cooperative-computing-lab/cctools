# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager

from ndcctools.taskvine.dagvine.blueprint_graph.adaptor import Adaptor
from ndcctools.taskvine.dagvine.blueprint_graph.proxy_library import ProxyLibrary
from ndcctools.taskvine.dagvine.blueprint_graph.proxy_functions import compute_single_key
from ndcctools.taskvine.dagvine.blueprint_graph.blueprint_graph import BlueprintGraph, TaskOutputRef, TaskOutputWrapper
from ndcctools.taskvine.dagvine.vine_graph.vine_graph_client import VineGraphClient

import cloudpickle
import os
import signal
import json
import random
import time
from pympler import asizeof


def context_loader_func(graph_pkl):
    graph = cloudpickle.loads(graph_pkl)

    return {
        "graph": graph,
    }


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
            "shift-disk-load": 0,
            "clean-redundant-replicas": 0,
            "max-cores": 1000000,
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
            "time-metrics-filename": 0,
            "enable-debug-log": 1,
            "auto-recovery": 1,
            "max-retry-attempts": 15,
            "retry-interval-sec": 1,
            "print-graph-details": 0,
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
        self.runtime_directory = cvine.vine_get_runtime_directory(self._taskvine)

        print(f"=== Manager name: {color_text(self.name, 92)}")
        print(f"=== Manager port: {color_text(self.port, 92)}")
        print(f"=== Runtime directory: {color_text(self.runtime_directory, 92)}")
        self._sigint_received = False

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

    def _rep_key(self, k, r):
        return k if r == 0 else ("__rep", r, k)

    def _replicate_graph(self, task_dict, target_keys, repeats):
        if repeats <= 1:
            return task_dict, target_keys
        if isinstance(task_dict, BlueprintGraph):
            old_bg = task_dict
            new_bg = BlueprintGraph()
            new_bg.callables = list(old_bg.callables)
            new_bg._callable_index = dict(old_bg._callable_index)
            for r in range(repeats):
                def rewriter(ref):
                    return TaskOutputRef(self._rep_key(ref.task_key, r), ref.path)

                def _rewrite(obj):
                    return old_bg._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, (func_id, args, kwargs) in old_bg.task_dict.items():
                    new_bg.add_task(self._rep_key(k, r), old_bg.callables[func_id], *_rewrite(args), **_rewrite(kwargs))
            new_bg.finalize()
            vine_targets = list(target_keys)
            for r in range(1, repeats):
                vine_targets.extend(self._rep_key(k, r) for k in target_keys if k in old_bg.task_dict)
            return new_bg, vine_targets
        else:
            temp_bg = BlueprintGraph()
            expanded = {}
            for r in range(repeats):
                def rewriter(ref):
                    return TaskOutputRef(self._rep_key(ref.task_key, r), ref.path)

                def _rewrite(obj):
                    return temp_bg._visit_task_output_refs(obj, rewriter, rewrite=True)

                for k, v in task_dict.items():
                    func, args, kwargs = v
                    expanded[self._rep_key(k, r)] = (func, _rewrite(args), _rewrite(kwargs))
            vine_targets = list(target_keys)
            for r in range(1, repeats):
                vine_targets.extend(self._rep_key(k, r) for k in target_keys if k in task_dict)
            return expanded, vine_targets

    def build_blueprint_graph(self, task_dict):
        if isinstance(task_dict, BlueprintGraph):
            bg = task_dict
        else:
            bg = BlueprintGraph()

            for k, v in task_dict.items():
                func, args, kwargs = v
                assert callable(func), f"Task {k} does not have a callable"
                bg.add_task(k, func, *args, **kwargs)

        bg.finalize()

        return bg

    def build_vine_graph(self, py_graph, target_keys):
        """Mirror the Python graph into VineGraph, preserving ordering and targets."""
        assert py_graph is not None, "Python graph must be built before building the VineGraph"

        vine_graph = VineGraphClient(self._taskvine)

        vine_graph.set_proxy_function(compute_single_key)

        # Tune both manager and vine_graph before we start adding nodes/edges.
        self.tune_manager()
        self.tune_vine_graph(vine_graph)

        topo_order = py_graph.get_topological_order()

        # Build the cross-language mapping as we walk the topo order.
        for k in topo_order:
            node_id = vine_graph.add_node(k)
            py_graph.pykey2cid[k] = node_id
            py_graph.cid2pykey[node_id] = k
            for pk in py_graph.parents_of[k]:
                vine_graph.add_dependency(pk, k)

        # Now that every node is present, mark which ones are final outputs.
        for k in target_keys:
            vine_graph.set_target(k)

        vine_graph.compute_topology_metrics()

        return vine_graph

    def build_graphs(self, task_dict, target_keys):
        """Create both the python side graph and its C counterpart, wiring outputs for later use."""
        # Build the python side graph.
        py_graph = self.build_blueprint_graph(task_dict)

        # filter out target keys that are not in the collection dict
        missing_keys = [k for k in target_keys if k not in py_graph.task_dict]
        if missing_keys:
            print(f"=== Warning: the following target keys are not in the graph: {','.join(map(str, missing_keys))}")
        target_keys = list(set(target_keys) - set(missing_keys))

        # Build the c side graph.
        vine_graph = self.build_vine_graph(py_graph, target_keys)

        # Cross-fill the outfile locations so the runtime graph knows where to read/write.
        for k in py_graph.pykey2cid:
            outfile_remote_name = vine_graph.get_node_outfile_remote_name(k)
            py_graph.outfile_remote_name[k] = outfile_remote_name

        # For each task, declare the input and output files in the vine graph
        for filename in py_graph.producer_of:
            task_key = py_graph.producer_of[filename]
            vine_graph.add_task_output(task_key, filename)
        for filename in py_graph.consumers_of:
            for task_key in py_graph.consumers_of[filename]:
                vine_graph.add_task_input(task_key, filename)

        return py_graph, vine_graph

    def create_proxy_library(self, py_graph, vine_graph, hoisting_modules, env_files):
        """Package up the python side graph as a TaskVine library."""
        proxy_library = ProxyLibrary(self)
        proxy_library.add_hoisting_modules(hoisting_modules)
        proxy_library.add_env_files(env_files)
        proxy_library.set_context_loader(context_loader_func, context_loader_args=[cloudpickle.dumps(py_graph)])
        proxy_library.set_libcores(self.param("libcores"))
        proxy_library.set_name(vine_graph.get_proxy_library_name())

        return proxy_library

    def run(self, task_dict, target_keys=[], params={}, hoisting_modules=[], env_files={}, adapt_dask=False, repeats=1):
        """High-level entry point: normalise input, build graphs, ship the library, execute, and return results."""
        time_start = time.time()

        # first update the params so that they can be used for the following construction
        self.update_params(params)

        if adapt_dask:
            task_dict = Adaptor(task_dict).converted

        result_keys = list(target_keys)
        task_dict, target_keys = self._replicate_graph(task_dict, target_keys, repeats)

        # Build both the Python DAG and its C mirror.
        py_graph, vine_graph = self.build_graphs(task_dict, target_keys)

        # set extra task output size and sleep time for each task
        for k in py_graph.task_dict:
            py_graph.extra_task_output_size_mb[k] = random.uniform(*self.param("extra-task-output-size-mb"))
            py_graph.extra_task_sleep_time[k] = random.uniform(*self.param("extra-task-sleep-time"))

        # Ship the execution context to workers via a proxy library
        proxy_library = self.create_proxy_library(py_graph, vine_graph, hoisting_modules, env_files)
        proxy_library.install()

        try:
            print(f"=== Library size: {asizeof.asizeof(proxy_library) / 1024 / 1024} MB")
            print(f"=== Frontend Overhead: {time.time() - time_start:.6f} seconds")

            vine_graph.execute()
            results = {}
            for k in result_keys:
                if k not in py_graph.task_dict:
                    continue
                outfile_path = os.path.join(self.param("output-dir"), py_graph.outfile_remote_name[k])
                results[k] = TaskOutputWrapper.load_from_path(outfile_path)
            makespan_s = round(vine_graph.get_makespan_us() / 1e6, 6)
            throughput_tps = round(len(py_graph.task_dict) / makespan_s, 6)
            results["Makespan"] = makespan_s
            results["Throughput"] = throughput_tps
            return results
        finally:
            try:
                proxy_library.uninstall()
            finally:
                vine_graph.delete()

    def _on_sigint(self, signum, frame):
        self._sigint_received = True
        raise KeyboardInterrupt
