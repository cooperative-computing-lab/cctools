
from ndcctools.taskvine import cvine
from ndcctools.taskvine.dagvine import cdagvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import delete_all_files, get_c_constant

from ndcctools.taskvine.dagvine.library import Library
from ndcctools.taskvine.dagvine.runtime_execution_graph import RuntimeExecutionGraph

import cloudpickle
import os
import signal

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


# convert Dask collection to task dictionary
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


# compatibility for Dask-created collections
def ensure_task_dict(collection_dict):
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
        self.vine_task_graph_tuning_params = {
            "failure-injection-step-percent": -1,
            "task-priority-mode": "largest-input-first",
            "proxy-library-name": "vine_task_graph_library",
            "proxy-function-name": "compute_single_key",
            "prune-depth": 1,
            "target-results-dir": "./target_results",
        }
        self.other_params = {
            "schedule": "worst",
            "libcores": 16,
            "failure-injection-step-percent": -1,
            "shared-file-system-dir": "./shared_file_system",
            "extra-task-output-size-mb": ["uniform", 0, 0],
            "extra-task-sleep-time": ["uniform", 0, 0],
            "outfile-type": {
                "temp": 1.0,
                "shared-file-system": 0.0,
            },
        }

    def update_param(self, param_name, new_value):
        if param_name in self.vine_manager_tuning_params:
            self.vine_manager_tuning_params[param_name] = new_value
        elif param_name in self.vine_task_graph_tuning_params:
            self.vine_task_graph_tuning_params[param_name] = new_value
        elif param_name in self.other_params:
            self.other_params[param_name] = new_value
        else:
            self.vine_manager_tuning_params[param_name] = new_value

    def get_value_of(self, param_name):
        if param_name in self.vine_manager_tuning_params:
            return self.vine_manager_tuning_params[param_name]
        elif param_name in self.vine_task_graph_tuning_params:
            return self.vine_task_graph_tuning_params[param_name]
        elif param_name in self.other_params:
            return self.other_params[param_name]
        else:
            raise ValueError(f"Invalid param name: {param_name}")


class GraphExecutor(Manager):
    def __init__(self,
                 *args,
                 **kwargs):

        # handle SIGINT correctly
        signal.signal(signal.SIGINT, self._on_sigint)

        self.params = GraphParams()

        # delete all files in the run info directory, do this before super().__init__()
        run_info_path = kwargs.get("run_info_path", None)
        run_info_template = kwargs.get("run_info_template", None)
        self.run_info_template_path = os.path.join(run_info_path, run_info_template)
        if self.run_info_template_path:
            delete_all_files(self.run_info_template_path)

        # initialize the manager
        super().__init__(*args, **kwargs)

        self.runtime_directory = cvine.vine_get_runtime_directory(self._taskvine)

    def tune_manager(self):
        for k, v in self.manager_tuning_params.to_dict().items():
            print(f"Tuning {k} to {v}")
            self.tune(k, v)

        # set worker scheduling algorithm
        cvine.vine_set_scheduler(self._taskvine, self.vine_constant_params.get_c_constant_of("schedule"))

    def param(self, param_name):
        return self.params.get_value_of(param_name)

    def update_params(self, new_params):
        assert isinstance(new_params, dict), "new_params must be a dict"
        for k, new_v in new_params.items():
            self.params.update_param(k, new_v)

    def get_run_info_path(self):
        return os.path.join(self.param("run-info-path"), self.param("run-info-template"))

    def tune_vine_manager(self):
        for k, v in self.params.vine_manager_tuning_params.items():
            print(f"Tuning {k} to {v}")
            self.tune(k, v)

    def tune_vine_task_graph(self, c_graph):
        for k, v in self.params.vine_task_graph_tuning_params.items():
            print(f"Tuning {k} to {v}")
            cdagvine.vine_task_graph_tune(c_graph, k, str(v))

    def assign_outfile_types(self, target_keys, reg, c_graph):
        assert reg is not None, "Python graph must be built first"
        assert c_graph is not None, "C graph must be built first"

        # get heavy score from C side
        heavy_scores = {}
        for k in reg.task_dict.keys():
            heavy_scores[k] = cdagvine.vine_task_graph_get_node_heavy_score(c_graph, reg.vine_key_of[k])

        # sort keys by heavy score descending
        sorted_keys = sorted(heavy_scores, key=lambda k: heavy_scores[k], reverse=True)

        # determine how many go to shared FS
        sharedfs_count = round(self.param("outfile-type")["shared-file-system"] * len(sorted_keys))

        # assign outfile types
        for i, k in enumerate(sorted_keys):
            if k in target_keys:
                choice = "local"
            elif i < sharedfs_count:
                choice = "shared-file-system"
            else:
                choice = "temp"

            reg.set_outfile_type_of(k, choice)
            outfile_type_enum = get_c_constant(f"NODE_OUTFILE_TYPE_{choice.upper().replace('-', '_')}")
            cdagvine.vine_task_graph_set_node_outfile(
                c_graph,
                reg.vine_key_of[k],
                outfile_type_enum,
                reg.outfile_remote_name[k]
            )

    def build_python_graph(self):
        reg = RuntimeExecutionGraph(
            self.task_dict,
            shared_file_system_dir=self.param("shared-file-system-dir"),
            extra_task_output_size_mb=self.param("extra-task-output-size-mb"),
            extra_task_sleep_time=self.param("extra-task-sleep-time")
        )

        return reg

    def build_c_graph(self, reg):
        assert reg is not None, "Python graph must be built before building the C graph"

        c_graph = cdagvine.vine_task_graph_create(self._taskvine)

        # C side vine task graph must be tuned before adding nodes and dependencies
        self.tune_vine_manager()
        self.tune_vine_task_graph(c_graph)

        topo_order = reg.get_topological_order()
        for k in topo_order:
            cdagvine.vine_task_graph_add_node(
                c_graph,
                reg.vine_key_of[k],
            )
            for pk in reg.parents_of[k]:
                cdagvine.vine_task_graph_add_dependency(c_graph, reg.vine_key_of[pk], reg.vine_key_of[k])

        cdagvine.vine_task_graph_compute_topology_metrics(c_graph)

        return c_graph

    def build_graphs(self, target_keys):
        # build Python DAG (logical topology)
        reg = self.build_python_graph()

        # build C DAG (physical topology)
        c_graph = self.build_c_graph(reg)

        # assign outfile types
        self.assign_outfile_types(target_keys, reg, c_graph)

        return reg, c_graph

    def run(self, collection_dict, target_keys=None, params={}, hoisting_modules=[], env_files={}):
        # first update the params so that they can be used for the following construction
        self.update_params(params)

        self.task_dict = ensure_task_dict(collection_dict)
        
        # build graphs in both Python and C sides
        reg, c_graph = self.build_graphs(target_keys)

        # create and install the library template on the manager
        library = Library(self, self.param("proxy-library-name"), self.param("libcores"))
        library.add_hoisting_modules(hoisting_modules)
        library.add_env_files(env_files)
        library.set_context_loader(RuntimeExecutionGraph.context_loader_func, context_loader_args=[cloudpickle.dumps(reg)])
        library.install()

        # execute the graph on the C side
        print(f"Executing task graph, logs will be written to {self.runtime_directory}")
        cdagvine.vine_task_graph_execute(c_graph)

        # clean up the library instances and template on the manager
        library.uninstall()
        # delete the C graph immediately after execution, so that the lifetime of this object is limited to the execution
        cdagvine.vine_task_graph_delete(c_graph)

        # load results of target keys
        results = {}
        for k in target_keys:
            results[k] = reg.load_result_of_key(k, target_results_dir=self.param("target-results-dir"))
        return results

    def _on_sigint(self, signum, frame):
        self.__del__()

