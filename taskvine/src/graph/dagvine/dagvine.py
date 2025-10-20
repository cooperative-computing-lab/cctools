
from ndcctools.taskvine import cvine
from ndcctools.taskvine.manager import Manager
from ndcctools.taskvine.utils import delete_all_files

from ndcctools.taskvine.dagvine.proxy_library import ProxyLibrary
from ndcctools.taskvine.dagvine.proxy_functions import compute_single_key
from ndcctools.taskvine.dagvine.runtime_execution_graph import RuntimeExecutionGraph, GraphKeyResult
from ndcctools.taskvine.dagvine.strategic_orchestration_graph import StrategicOrchestrationGraph

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
        self.sog_tuning_params = {
            "failure-injection-step-percent": -1,
            "task-priority-mode": "largest-input-first",
            "prune-depth": 1,
            "output-dir": "./outputs",
            "checkpoint-dir": "./checkpoints",
            "checkpoint-fraction": 0,
        }
        self.other_params = {
            "schedule": "worst",
            "libcores": 16,
            "failure-injection-step-percent": -1,
            "extra-task-output-size-mb": ["uniform", 0, 0],
            "extra-task-sleep-time": ["uniform", 0, 0],
        }

    def update_param(self, param_name, new_value):
        if param_name in self.vine_manager_tuning_params:
            self.vine_manager_tuning_params[param_name] = new_value
        elif param_name in self.sog_tuning_params:
            self.sog_tuning_params[param_name] = new_value
        elif param_name in self.other_params:
            self.other_params[param_name] = new_value
        else:
            self.vine_manager_tuning_params[param_name] = new_value

    def get_value_of(self, param_name):
        if param_name in self.vine_manager_tuning_params:
            return self.vine_manager_tuning_params[param_name]
        elif param_name in self.sog_tuning_params:
            return self.sog_tuning_params[param_name]
        elif param_name in self.other_params:
            return self.other_params[param_name]
        else:
            raise ValueError(f"Invalid param name: {param_name}")


class DAGVine(Manager):
    def __init__(self,
                 *args,
                 **kwargs):

        # handle SIGINT
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

    def param(self, param_name):
        return self.params.get_value_of(param_name)

    def update_params(self, new_params):
        assert isinstance(new_params, dict), "new_params must be a dict"
        for k, new_v in new_params.items():
            self.params.update_param(k, new_v)

    def get_run_info_path(self):
        return os.path.join(self.param("run-info-path"), self.param("run-info-template"))

    def tune_manager(self):
        for k, v in self.params.vine_manager_tuning_params.items():
            try:
                self.tune(k, v)
            except Exception:
                raise ValueError(f"Unrecognized parameter: {k}")

    def tune_sog(self, sog):
        for k, v in self.params.sog_tuning_params.items():
            sog.tune(k, str(v))

    def build_reg(self, task_dict):
        reg = RuntimeExecutionGraph(
            task_dict,
            extra_task_output_size_mb=self.param("extra-task-output-size-mb"),
            extra_task_sleep_time=self.param("extra-task-sleep-time")
        )

        return reg

    def build_sog(self, reg, target_keys):
        assert reg is not None, "Python graph must be built before building the C graph"

        sog = StrategicOrchestrationGraph(self._taskvine)

        sog.set_proxy_function(compute_single_key)

        # C side vine task graph must be tuned before adding nodes and dependencies
        self.tune_manager()
        self.tune_sog(sog)

        topo_order = reg.get_topological_order()
        for k in topo_order:
            sog.add_node(reg.reg_key_to_sog_key[k], int(k in target_keys))
            for pk in reg.parents_of[k]:
                sog.add_dependency(reg.reg_key_to_sog_key[pk], reg.reg_key_to_sog_key[k])

        sog.compute_topology_metrics()

        return sog

    def build_graphs(self, task_dict, target_keys):
        # build Python DAG (logical topology)
        reg = self.build_reg(task_dict)
        # build C DAG (physical topology)
        sog = self.build_sog(reg, target_keys)

        # set outfile remote names in reg from sog, note that these names are automatically generated
        # with regard to the checkpointing strategy and the shared file system directory
        for sog_key in reg.reg_key_to_sog_key.values():
            outfile_remote_name = sog.get_node_outfile_remote_name(sog_key)
            reg.set_outfile_remote_name_of(reg.sog_key_to_reg_key[sog_key], outfile_remote_name)

        return reg, sog

    def create_proxy_library(self, reg, sog, hoisting_modules, env_files):
        proxy_library = ProxyLibrary(self)
        proxy_library.add_hoisting_modules(hoisting_modules)
        proxy_library.add_env_files(env_files)
        proxy_library.set_context_loader(RuntimeExecutionGraph.context_loader_func, context_loader_args=[cloudpickle.dumps(reg)])
        proxy_library.set_libcores(self.param("libcores"))
        proxy_library.set_name(sog.get_proxy_library_name())

        return proxy_library

    def run(self, collection_dict, target_keys=[], params={}, hoisting_modules=[], env_files={}):
        # first update the params so that they can be used for the following construction
        self.update_params(params)

        task_dict = ensure_task_dict(collection_dict)

        # build graphs from both sides
        reg, sog = self.build_graphs(task_dict, target_keys)

        # create and install the proxy library on the manager
        proxy_library = self.create_proxy_library(reg, sog, hoisting_modules, env_files)
        proxy_library.install()

        # execute the graph on the C side
        print(f"Executing task graph, logs will be written to {self.runtime_directory}")
        sog.execute()

        # clean up the library instances and template on the manager
        proxy_library.uninstall()

        # delete the C graph immediately after execution, so that the lifetime of this object is limited to the execution
        sog.delete()

        # load results of target keys
        results = {}
        for k in target_keys:
            outfile_path = os.path.join(self.param("output-dir"), reg.outfile_remote_name[k])
            results[k] = GraphKeyResult.load_from_path(outfile_path)
        return results

    def _on_sigint(self, signum, frame):
        self.__del__()
