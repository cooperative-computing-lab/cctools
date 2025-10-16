from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import delete_all_files, get_c_constant

from ndcctools.taskvine.dagvine.graph_definition import GraphKeyResult, TaskGraph
from ndcctools.taskvine.dagvine.params import ManagerTuningParams
from ndcctools.taskvine.dagvine.params import VineConstantParams
from ndcctools.taskvine.dagvine.params import RegularParams

import cloudpickle
import os
import collections
import inspect
import types
import signal
import hashlib
import time
import random
import uuid

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


def dask_collections_to_task_dict(collection_dict):
    assert is_dask_collection is not None
    from dask.highlevelgraph import HighLevelGraph, ensure_dict

    if not isinstance(collection_dict, dict):
        raise TypeError("Input must be a dict or a HighLevelGraph")

    for k, v in collection_dict.items():
        if not is_dask_collection(v):
            raise TypeError(f"Input must be a dict of DaskCollection, but found {k} with type {type(v)}")

    if dts:
        sub_hlgs = [v.dask for v in collection_dict.values()]
        hlg = HighLevelGraph.merge(*sub_hlgs).to_dict()
    else:
        hlg = dask.base.collections_to_dsk(collection_dict.values())

    return ensure_dict(hlg)


def ensure_task_dict(collection_dict):
    if is_dask_collection and any(is_dask_collection(v) for v in collection_dict.values()):
        task_dict = dask_collections_to_task_dict(collection_dict)
    else:
        task_dict = collection_dict

    if dts:
        return dts.convert_legacy_graph(task_dict)
    else:
        return task_dict


class GraphExecutor(Manager):
    def __init__(self,
                 *args,
                 manager_tuning_params=ManagerTuningParams(),
                 regular_params=RegularParams(),
                 vine_constant_params=VineConstantParams(),
                 **kwargs):

        signal.signal(signal.SIGINT, self._on_sigint)

        self.manager_tuning_params = manager_tuning_params
        self.vine_constant_params = vine_constant_params
        self.regular_params = regular_params

        # delete all files in the run info directory, do this before super().__init__()
        run_info_path = self.regular_params.run_info_path
        run_info_template = self.regular_params.run_info_template
        self.run_info_path_absolute = os.path.join(run_info_path, run_info_template)
        if run_info_path and run_info_template:
            delete_all_files(self.run_info_path_absolute)

        kwargs["run_info_path"] = run_info_path
        kwargs["run_info_template"] = run_info_template
        super().__init__(*args, **kwargs)
        print(f"TaskVine Manager \033[92m{self.name}\033[0m listening on port \033[92m{self.port}\033[0m")

        # initialize the task graph
        self._vine_task_graph = cvine.vine_task_graph_create(self._taskvine)

    def tune_manager(self):
        for k, v in self.manager_tuning_params.to_dict().items():
            print(f"Tuning {k} to {v}")
            self.tune(k, v)

    def set_policy(self):
        # set replica placement policy
        cvine.vine_set_replica_placement_policy(self._taskvine, self.vine_constant_params.get_c_constant_of("replica_placement_policy"))
        # set worker scheduling algorithm
        cvine.vine_set_scheduler(self._taskvine, self.vine_constant_params.get_c_constant_of("schedule"))
        # set task priority mode
        cvine.vine_task_graph_set_task_priority_mode(self._vine_task_graph, self.vine_constant_params.get_c_constant_of("task_priority_mode"))

    def run(self,
            collection_dict,
            target_keys=[],
            library=None,
            ):

        # tune the manager every time we start a new run as the parameters may have changed
        self.tune_manager()
        # set the policy every time we start a new run as the parameters may have changed
        self.set_policy()

        self.target_keys = target_keys
        self.task_dict = ensure_task_dict(collection_dict)

        # create library task
        self.library = library
        self.library.install(self, self.regular_params.libcores, self._vine_task_graph)

        cvine.vine_task_graph_set_failure_injection_step_percent(self._vine_task_graph, self.regular_params.failure_injection_step_percent)

        # create task graph in the python side
        print("Initializing TaskGraph object")
        self.task_graph = TaskGraph(self.task_dict,
                                    staging_dir=self.regular_params.staging_dir,
                                    shared_file_system_dir=self.regular_params.shared_file_system_dir,
                                    extra_task_output_size_mb=self.regular_params.extra_task_output_size_mb,
                                    extra_task_sleep_time=self.regular_params.extra_task_sleep_time)
        topo_order = self.task_graph.get_topological_order()

        # create task graph in the python side
        print("Initializing task graph in TaskVine")
        for k in topo_order:
            cvine.vine_task_graph_add_node(self._vine_task_graph,
                                           self.task_graph.vine_key_of[k],
                                           self.regular_params.staging_dir,
                                           self.regular_params.prune_depth)
            for pk in self.task_graph.parents_of.get(k, []):
                cvine.vine_task_graph_add_dependency(self._vine_task_graph, self.task_graph.vine_key_of[pk], self.task_graph.vine_key_of[k])

        # we must finalize the graph in c side after all nodes and dependencies are added
        # this includes computing various metrics for each node, such as depth, height, heavy score, etc.
        cvine.vine_task_graph_compute_topology_metrics(self._vine_task_graph)

        # then we can use the heavy score to sort the nodes and specify their outfile remote names
        heavy_scores = {}
        for k in self.task_graph.task_dict.keys():
            heavy_scores[k] = cvine.vine_task_graph_get_node_heavy_score(self._vine_task_graph, self.task_graph.vine_key_of[k])

        # keys with larger heavy score should be stored into the shared file system
        sorted_keys = sorted(heavy_scores, key=lambda x: heavy_scores[x], reverse=True)
        shared_file_system_size = round(len(sorted_keys) * self.regular_params.outfile_type["shared-file-system"])
        for i, k in enumerate(sorted_keys):
            if k in self.target_keys:
                choice = "local"
            else:
                if i < shared_file_system_size:
                    choice = "shared-file-system"
                else:
                    choice = "temp"
            # set on the Python side, will be installed on the remote workers
            self.task_graph.set_outfile_type_of(k, choice)
            # set on the C side, so the manager knows where the data is stored
            outfile_type_str = f"NODE_OUTFILE_TYPE_{choice.upper().replace('-', '_')}"
            cvine.vine_task_graph_set_node_outfile(self._vine_task_graph,
                                                   self.task_graph.vine_key_of[k],
                                                   get_c_constant(outfile_type_str),
                                                   self.task_graph.outfile_remote_name[k])

        # save the task graph to a pickle file, will be sent to the remote workers
        with open(self.library.local_path, 'wb') as f:
            cloudpickle.dump(self.task_graph, f)

        # now execute the vine graph
        print(f"\033[92mExecuting task graph, logs will be written into {self.run_info_path_absolute}\033[0m")
        cvine.vine_task_graph_execute(self._vine_task_graph)

        # after execution, we need to load results of target keys
        results = {}
        for k in self.target_keys:
            local_outfile_path = cvine.vine_task_graph_get_node_local_outfile_source(self._vine_task_graph, self.task_graph.vine_key_of[k])
            if not os.path.exists(local_outfile_path):
                results[k] = "NOT_FOUND"
                continue
            with open(local_outfile_path, 'rb') as f:
                result_obj = cloudpickle.load(f)
                assert isinstance(result_obj, GraphKeyResult), "Loaded object is not of type GraphKeyResult"
                results[k] = result_obj.result
        return results

    def _on_sigint(self, signum, frame):
        self.__del__()

    def __del__(self):
        if hasattr(self, '_vine_task_graph') and self._vine_task_graph:
            cvine.vine_task_graph_delete(self._vine_task_graph)

        if hasattr(self, 'library') and self.library.local_path and os.path.exists(self.library.local_path):
            os.remove(self.library.local_path)
