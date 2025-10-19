# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import hashlib
import time
import cloudpickle
import collections
import uuid
import random
from collections import deque

try:
    import dask
except ImportError:
    dask = None

try:
    import dask._task_spec as dts
except ImportError:
    dts = None


def hash_name(*args):
    out_str = ""
    for arg in args:
        out_str += str(arg)
    return hashlib.sha256(out_str.encode('utf-8')).hexdigest()[:32]


def hashable(s):
    try:
        hash(s)
        return True
    except TypeError:
        return False


def dist_func(mode, low, high):
    """ Generate a random value from a distribution. """
    if not mode:
        return 0

    assert mode in ["uniform", "normal", "lognormal", "pareto", "mix"]

    # uniform distribution, flat spread
    def uniform_dist():
        return random.uniform(low, high)

    # normal distribution, centered in the middle
    def normal_dist():
        mu, sigma = (low + high) / 2, (high - low) / 6
        return min(max(random.gauss(mu, sigma), low), high)

    # lognormal distribution, long tail
    def lognormal_dist():
        val = random.lognormvariate(0, 1)
        val = val / (1 + val)
        return low + (high - low) * val

    # pareto distribution, very heavy tail
    def pareto_dist(alpha=2.0):
        val = random.paretovariate(alpha)
        val = val / (1 + val)
        return low + (high - low) * val

    # mixture: most small values, few large ones
    def mix_dist():
        if random.random() < 0.9:
            return random.uniform(low, (low + high) / 2)
        else:
            return random.uniform((low + high) / 2, high)

    return {
        "uniform": uniform_dist,
        "normal": normal_dist,
        "lognormal": lognormal_dist,
        "pareto": pareto_dist,
        "mix": mix_dist,
    }[mode]()


class GraphKeyResult:
    """ A wrapper class to store the result of a task and the extra size in MB to allocate for this object in testing mode to evaluate storage consumption
    and peer transfer performance across all workers. """
    def __init__(self, result, extra_size_mb=None):
        self.result = result
        self.extra_obj = bytearray(int(extra_size_mb * 1024 * 1024)) if extra_size_mb and extra_size_mb > 0 else None

    @staticmethod
    def load_from_path(path):
        try:
            with open(path, "rb") as f:
                result_obj = cloudpickle.load(f)
                assert isinstance(result_obj, GraphKeyResult), "Loaded object is not of type GraphKeyResult"
                return result_obj.result
        except FileNotFoundError:
            raise FileNotFoundError(f"Output file not found at {path}")


class RuntimeExecutionGraph:
    """
    The RuntimeExecutionGraph class constructs the task graph and manages task dependencies.
    This class is cloudpickled and sent to workers, where it is hoisted by the library instance.
    The global RuntimeExecutionGraph object then serves as the execution context: whenever a task key is invoked,
    the system resolves the corresponding graph key from the Vine key and executes the mapped function
    to produce the result.
    """
    def __init__(self, task_dict,
                 shared_file_system_dir=None,
                 extra_task_output_size_mb=["uniform", 0, 0],
                 extra_task_sleep_time=["uniform", 0, 0]):
        self.task_dict = task_dict
        self.shared_file_system_dir = shared_file_system_dir

        if self.shared_file_system_dir:
            os.makedirs(self.shared_file_system_dir, exist_ok=True)

        if dts:
            for k, v in self.task_dict.items():
                if isinstance(v, dts.GraphNode):
                    assert isinstance(v, (dts.Alias, dts.Task, dts.DataNode)), f"Unsupported task type for key {k}: {v.__class__}"

        self.parents_of, self.children_of = self._build_dependencies(self.task_dict)
        self.depth_of = self._calculate_depths()

        self.sog_node_key_of = {k: hash_name(k) for k in task_dict.keys()}
        self.reg_node_key_of = {hash_name(k): k for k in task_dict.keys()}

        # will be set from sog
        self.outfile_remote_name = {key: None for key in self.task_dict.keys()}

        # testing params
        self.extra_task_output_size_mb = self._calculate_extra_size_mb_of(extra_task_output_size_mb)
        self.extra_sleep_time_of = self._calculate_extra_sleep_time_of(extra_task_sleep_time)

    def _calculate_extra_size_mb_of(self, extra_task_output_size_mb):
        assert isinstance(extra_task_output_size_mb, list) and len(extra_task_output_size_mb) == 3
        mode, low, high = extra_task_output_size_mb
        low, high = int(low), int(high)
        assert low <= high

        max_depth = max(depth for depth in self.depth_of.values())
        extra_size_mb_of = {}
        for k in self.task_dict.keys():
            if self.depth_of[k] == max_depth or self.depth_of[k] == max_depth - 1:
                extra_size_mb_of[k] = 0
                continue
            extra_size_mb_of[k] = dist_func(mode, low, high)

        return extra_size_mb_of

    def _calculate_extra_sleep_time_of(self, extra_task_sleep_time):
        assert isinstance(extra_task_sleep_time, list) and len(extra_task_sleep_time) == 3
        mode, low, high = extra_task_sleep_time
        low, high = int(low), int(high)
        assert low <= high

        extra_sleep_time_of = {}
        for k in self.task_dict.keys():
            extra_sleep_time_of[k] = dist_func(mode, low, high)

        return extra_sleep_time_of

    def _calculate_depths(self):
        depth_of = {key: 0 for key in self.task_dict.keys()}

        topo_order = self.get_topological_order()
        for key in topo_order:
            if self.parents_of[key]:
                depth_of[key] = max(depth_of[parent] for parent in self.parents_of[key]) + 1
            else:
                depth_of[key] = 0

        return depth_of

    def set_outfile_remote_name_of(self, key, outfile_remote_name):
        self.outfile_remote_name[key] = outfile_remote_name

    def is_dts_key(self, k):
        if not hasattr(dask, "_task_spec"):
            return False
        import dask._task_spec as dts
        return isinstance(self.task_dict[k], (dts.Task, dts.TaskRef, dts.Alias, dts.DataNode, dts.NestedContainer))

    def _build_dependencies(self, task_dict):
        def _find_sexpr_parents(sexpr):
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
        with open(self.outfile_remote_name[key], "wb") as f:
            result_obj = GraphKeyResult(result, extra_size_mb=self.extra_task_output_size_mb[key])
            cloudpickle.dump(result_obj, f)

    def load_result_of_key(self, key):
        # workers user this function to load results from either local or shared file system
        # if a node-local output, then data is stored in the task sandbox and the remote name is just the filename
        # if a shared file system output, then remote name is the full path to the file
        outfile_path = self.outfile_remote_name[key]
        return GraphKeyResult.load_from_path(outfile_path)

    def get_topological_order(self):
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
    def context_loader_func(reg_pkl):
        reg = cloudpickle.loads(reg_pkl)

        if not isinstance(reg, RuntimeExecutionGraph):
            raise TypeError("reg_pkl is not of type RuntimeExecutionGraph")

        return {
            "reg": reg,
        }

