from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import load_variable_from_library, delete_all_files, get_c_constant
from ndcctools.taskvine.graph_definition import GraphKeyResult, TaskGraph, init_task_graph_context, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable

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
                 libcores=1,
                 hoisting_modules=[],
                 libtask_env_files={},
                 **kwargs):

        signal.signal(signal.SIGINT, self._on_sigint)

        # delete all files in the run info template directory, do this before super().__init__()
        self.run_info_path = kwargs.get('run_info_path')
        self.run_info_template = kwargs.get('run_info_template')
        self.run_info_path_absolute = os.path.join(self.run_info_path, self.run_info_template)
        if self.run_info_path and self.run_info_template:
            delete_all_files(self.run_info_path_absolute)

        # initialize the manager
        super_params = set(inspect.signature(Manager.__init__).parameters)
        super_kwargs = {k: v for k, v in kwargs.items() if k in super_params}

        super().__init__(*args, **super_kwargs)
        print(f"TaskVine manager \033[92m{self.name}\033[0m listening on port \033[92m{self.port}\033[0m")

        # tune the manager
        leftover_kwargs = {k: v for k, v in kwargs.items() if k not in super_params}
        for key, value in leftover_kwargs.items():
            try:
                vine_param = key.replace("_", "-")
                self.tune(vine_param, value)
                print(f"Tuned {vine_param} to {value}")
            except Exception as e:
                print(f"Failed to tune {key} with value {value}: {e}")
                exit(1)
        self.tune("worker-source-max-transfers", 100)
        self.tune("max-retrievals", -1)
        self.tune("prefer-dispatch", 1)
        self.tune("transient-error-interval", 1)
        self.tune("attempt-schedule-depth", 1000)

        # initialize the task graph
        self._vine_task_graph = cvine.vine_task_graph_create(self._taskvine)

        # create library task with specified resources
        self._create_library_task(libcores, hoisting_modules, libtask_env_files)

    def _create_library_task(self, libcores=1, hoisting_modules=[], libtask_env_files={}):
        assert cvine.vine_task_graph_get_proxy_function_name(self._vine_task_graph) == compute_single_key.__name__

        self.task_graph_pkl_file_name = f"library-task-graph-{uuid.uuid4()}.pkl"
        self.task_graph_pkl_file_local_path = self.task_graph_pkl_file_name
        self.task_graph_pkl_file_remote_path = self.task_graph_pkl_file_name

        hoisting_modules += [os, cloudpickle, GraphKeyResult, TaskGraph, uuid, hashlib, random, types, collections, time,
                             load_variable_from_library, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable]
        if dask:
            hoisting_modules += [dask]
        self.libtask = self.create_library_from_functions(
            cvine.vine_task_graph_get_proxy_library_name(self._vine_task_graph),
            compute_single_key,
            library_context_info=[init_task_graph_context, [], {"task_graph_path": self.task_graph_pkl_file_remote_path}],
            add_env=False,
            function_infile_load_mode="json",
            hoisting_modules=hoisting_modules
        )
        self.libtask.add_input(self.declare_file(self.task_graph_pkl_file_local_path), self.task_graph_pkl_file_remote_path)
        for local_file_path, remote_file_path in libtask_env_files.items():
            self.libtask.add_input(self.declare_file(local_file_path, cache=True, peer_transfer=True), remote_file_path)
        self.libtask.set_cores(libcores)
        self.libtask.set_function_slots(libcores)
        self.install_library(self.libtask)

    def run(self,
            collection_dict,
            target_keys=[],
            replica_placement_policy="random",
            priority_mode="largest-input-first",
            scheduling_mode="files",
            extra_task_output_size_mb=["uniform", 0, 0],
            extra_task_sleep_time=["uniform", 0, 0],
            prune_depth=1,
            shared_file_system_dir="/project01/ndcms/jzhou24/shared_file_system",
            staging_dir="/project01/ndcms/jzhou24/staging",
            failure_injection_step_percent=-1,
            balance_worker_disk_load=0,
            outfile_type={
                "temp": 1.0,
                "shared-file-system": 0.0,
            }):
        self.target_keys = target_keys
        self.task_dict = ensure_task_dict(collection_dict)

        self.tune("balance-worker-disk-load", balance_worker_disk_load)

        cvine.vine_task_graph_set_failure_injection_step_percent(self._vine_task_graph, failure_injection_step_percent)

        if balance_worker_disk_load:
            replica_placement_policy = "disk-load"
            scheduling_mode = "worst"

        self.set_scheduler(scheduling_mode)

        # create task graph in the python side
        print("Initializing TaskGraph object")
        self.shared_file_system_dir = shared_file_system_dir
        self.staging_dir = staging_dir
        self.task_graph = TaskGraph(self.task_dict,
                                    staging_dir=self.staging_dir,
                                    shared_file_system_dir=self.shared_file_system_dir,
                                    extra_task_output_size_mb=extra_task_output_size_mb,
                                    extra_task_sleep_time=extra_task_sleep_time)
        topo_order = self.task_graph.get_topological_order()

        # the sum of the values in outfile_type must be 1.0
        assert sum(list(outfile_type.values())) == 1.0

        # set replica placement policy
        cvine.vine_set_replica_placement_policy(self._taskvine, get_c_constant(f"replica_placement_policy_{replica_placement_policy.replace('-', '_')}"))

        # create task graph in the python side
        print("Initializing task graph in TaskVine")
        for k in topo_order:
            cvine.vine_task_graph_create_node(self._vine_task_graph,
                                              self.task_graph.vine_key_of[k],
                                              self.staging_dir,
                                              prune_depth,
                                              get_c_constant(f"task_priority_mode_{priority_mode.replace('-', '_')}"))
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
        shared_file_system_size = round(len(sorted_keys) * outfile_type["shared-file-system"])
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
        with open(self.task_graph_pkl_file_local_path, 'wb') as f:
            cloudpickle.dump(self.task_graph, f)

        # now execute the vine graph
        print(f"Executing task graph, logs will be written into {self.run_info_path_absolute}")
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

        if hasattr(self, 'task_graph_pkl_file_local_path') and os.path.exists(self.task_graph_pkl_file_local_path):
            os.remove(self.task_graph_pkl_file_local_path)
