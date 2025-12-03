# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import cloudpickle
import collections
import random
from collections import deque

# Attempt to import Dask helpers. When they are unavailable we fall back to
# None so environments without Dask continue to work. If Dask is present—either
# the legacy graph or the newer TaskSpec API—we normalize it into our internal
# task representation.
try:
    import dask
except ImportError:
    dask = None
try:
    import dask._task_spec as dts
except ImportError:
    dts = None


def hashable(s):
    """Used while wiring dependencies to spot values we can treat as node keys."""
    try:
        hash(s)
        return True
    except TypeError:
        return False


# Lightweight wrapper around task results that optionally pads the payload. The
# padding lets tests model large outputs without altering the logical result.
class ContextGraphTaskResult:
    def __init__(self, result, extra_size_mb=None):
        """Store the real user result plus optional padding used during regression tests."""
        self.result = result
        self.extra_obj = bytearray(int(extra_size_mb * 1024 * 1024)) if extra_size_mb and extra_size_mb > 0 else None

    @staticmethod
    def load_from_path(path):
        """Workers call this while recovering an output produced by save_result_of_key from disk.
        If a node-local output, then data is stored in the task sandbox and the path is just the filename
        If a shared file system output, then path is the full path to the file
        If a target result, the path is the full path to the file in the manager's output directory"""
        try:
            with open(path, "rb") as f:
                result_obj = cloudpickle.load(f)
                assert isinstance(result_obj, ContextGraphTaskResult), "Loaded object is not of type ContextGraphTaskResult"
                return result_obj.result
        except FileNotFoundError:
            raise FileNotFoundError(f"Output file not found at {path}")


# ContextGraph builds the logical DAG and manages dependencies. The
# object is cloudpickled, shipped with the proxy library, and hoisted on worker
# nodes. When a task key executes we map from the Vine key back to the original
# graph key, run the user function, and persist the result.
class ContextGraph:
    def __init__(self, task_dict,
                 extra_task_output_size_mb=[0, 0],
                 extra_task_sleep_time=[0, 0]):
        """Capture the Python DAG that DAGVine hands us before we mirror it in C."""
        self.task_dict = task_dict

        if dts:
            for k, v in self.task_dict.items():
                if isinstance(v, dts.GraphNode):
                    assert isinstance(v, (dts.Alias, dts.Task, dts.DataNode)), f"Unsupported task type for key {k}: {v.__class__}"

        self.parents_of, self.children_of = self._build_dependencies(self.task_dict)

        # these mappings are set after node ids are assigned in the C vine graph
        self.ckey2vid = {}
        self.vid2ckey = {}

        # will be set from vine graph
        self.outfile_remote_name = {key: None for key in self.task_dict.keys()}

        # testing params
        self.extra_task_output_size_mb = self._calculate_extra_size_mb_of(extra_task_output_size_mb)
        self.extra_sleep_time_of = self._calculate_extra_sleep_time_of(extra_task_sleep_time)

    def _calculate_extra_size_mb_of(self, extra_task_output_size_mb):
        """Sample a uniform byte budget between low/high for every node."""
        assert isinstance(extra_task_output_size_mb, list) and len(extra_task_output_size_mb) == 2
        low, high = extra_task_output_size_mb
        low, high = int(low), int(high)
        assert low <= high

        return {k: random.uniform(low, high) for k in self.task_dict.keys()}

    def _calculate_extra_sleep_time_of(self, extra_task_sleep_time):
        """Pick a uniform delay between low/high so tests can fake runtime."""
        assert isinstance(extra_task_sleep_time, list) and len(extra_task_sleep_time) == 2
        low, high = extra_task_sleep_time
        low, high = int(low), int(high)
        assert low <= high

        return {k: random.uniform(low, high) for k in self.task_dict.keys()}

    def is_dts_key(self, k):
        """Gate the Dask-specific branch when we parse task definitions."""
        if not hasattr(dask, "_task_spec"):
            return False
        import dask._task_spec as dts
        return isinstance(self.task_dict[k], (dts.Task, dts.TaskRef, dts.Alias, dts.DataNode, dts.NestedContainer))

    def _build_dependencies(self, task_dict):
        """Normalize mixed Dask/s-expression inputs into our parent/child lookup tables."""
        def _find_sexpr_parents(sexpr):
            """Resolve the immediate parents inside one symbolic expression node."""
            if hashable(sexpr) and sexpr in task_dict.keys():
                return {sexpr}
            elif isinstance(sexpr, (list, tuple)):
                deps = set()
                for x in sexpr:
                    deps |= _find_sexpr_parents(x)
                return deps
            elif isinstance(sexpr, dict):
                deps = set()
                for k, v in sexpr.items():
                    deps |= _find_sexpr_parents(k)
                    deps |= _find_sexpr_parents(v)
                return deps
            else:
                return set()

        parents_of = collections.defaultdict(set)
        children_of = collections.defaultdict(set)

        for k, value in task_dict.items():
            if self.is_dts_key(k):
                # in the new Dask expression, each value is an object from dask._task_spec, could be
                # a Task, Alias, TaskRef, etc., but they all share the same base class the dependencies
                # field is of type frozenset(), without recursive ancestor dependencies involved
                parents_of[k] = value.dependencies
            else:
                # the value could be a sexpr, e.g., the old Dask representation
                parents_of[k] = _find_sexpr_parents(value)

        for k, deps in parents_of.items():
            for dep in deps:
                children_of[dep].add(k)

        return parents_of, children_of

    def save_result_of_key(self, key, result):
        """Called from the proxy function to persist a result into disk after the worker finishes."""
        with open(self.outfile_remote_name[key], "wb") as f:
            result_obj = ContextGraphTaskResult(result, extra_size_mb=self.extra_task_output_size_mb[key])
            cloudpickle.dump(result_obj, f)

    def load_result_of_key(self, key):
        """Used by downstream tasks to pull inputs from disk or the shared store."""
        # workers user this function to load results from either local or shared file system
        # if a node-local output, then data is stored in the task sandbox and the remote name is just the filename
        # if a shared file system output, then remote name is the full path to the file
        outfile_path = self.outfile_remote_name[key]
        return ContextGraphTaskResult.load_from_path(outfile_path)

    def get_topological_order(self):
        """Produce the order DAGVine uses when assigning node IDs to the C graph."""
        in_degree = {key: len(self.parents_of[key]) for key in self.task_dict.keys()}
        queue = deque([key for key, degree in in_degree.items() if degree == 0])
        topo_order = []

        while queue:
            current = queue.popleft()
            topo_order.append(current)

            for child in self.children_of[current]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(topo_order) != len(self.task_dict):
            print(f"len(topo_order): {len(topo_order)}")
            print(f"len(self.task_dict): {len(self.task_dict)}")
            raise ValueError("Failed to create topo order, the dependencies may be cyclic or problematic")

        return topo_order

    @staticmethod
    def context_loader_func(context_graph_pkl):
        """Entry point the proxy library invokes to restore the serialized ContextGraph."""
        context_graph = cloudpickle.loads(context_graph_pkl)

        if not isinstance(context_graph, ContextGraph):
            raise TypeError("context_graph_pkl is not of type ContextGraph")

        return {
            "context_graph": context_graph,
        }
