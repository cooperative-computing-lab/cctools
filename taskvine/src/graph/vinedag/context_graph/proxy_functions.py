# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import time
from ndcctools.taskvine.utils import load_variable_from_library


def compute_dts_key(context_graph, k, v):
    """
    Compute the result of a Dask task node from dask._task_spec.

    Each value `v` may be an instance of Task, Alias, or DataNode, all of which
    inherit from the same base class. The `dependencies` field is a frozenset
    containing direct dependencies only (no recursive ancestry).

    The function resolves each dependency from the context_graph, constructs an
    input dictionary, and then executes the node according to its type.
    """
    try:
        import dask._task_spec as dts
    except ImportError:
        raise ImportError("Dask is not installed")

    input_dict = {dep: context_graph.load_result_of_key(dep) for dep in v.dependencies}

    try:
        if isinstance(v, dts.Alias):
            assert len(v.dependencies) == 1, "Expected exactly one dependency"
            return context_graph.load_result_of_key(next(iter(v.dependencies)))
        elif isinstance(v, dts.Task):
            return v(input_dict)
        elif isinstance(v, dts.DataNode):
            return v.value
        else:
            raise TypeError(f"unexpected node type: {type(v)} for key {k}")
    except Exception as e:
        raise Exception(f"Error while executing task {k}: {e}")


def compute_sexpr_key(context_graph, k, v):
    """
    Evaluate a symbolic expression (S-expression) task within the task graph.

    Both VineDAG and legacy Dask represent computations as symbolic
    expression trees (S-expressions). Each task value `v` encodes a nested
    structure where:
      - Leaf nodes are constants or task keys referencing parent results.
      - Lists are recursively evaluated.
      - Tuples of the form (func, arg1, arg2, ...) represent function calls.

    This function builds an input dictionary from all parent keys, then
    recursively resolves and executes the expression until a final value
    is produced.
    """
    input_dict = {parent: context_graph.load_result_of_key(parent) for parent in context_graph.parents_of[k]}

    def _rec_call(expr):
        try:
            if expr in input_dict.keys():
                return input_dict[expr]
        except TypeError:
            pass
        if isinstance(expr, list):
            return [_rec_call(e) for e in expr]
        if isinstance(expr, tuple) and len(expr) > 0 and callable(expr[0]):
            res = expr[0](*[_rec_call(a) for a in expr[1:]])
            return res
        return expr

    try:
        return _rec_call(v)
    except Exception as e:
        raise Exception(f"Failed to invoke _rec_call(): {e}")


def compute_single_key(vine_key):
    """
    Compute a single task identified by a Vine key within the current ContextGraph.

    The function retrieves the corresponding graph key and task object from the
    global context_graph, determines the task type, and dispatches to the appropriate
    execution interface — e.g., `compute_dts_key` for Dask-style task specs or
    `compute_sexpr_key` for S-expression graphs.

    This design allows extensibility: for new graph representations, additional
    compute interfaces can be introduced and registered here to handle new key types.

    After computation, the result is saved, the output file is validated, and
    an optional delay (`extra_sleep_time_of`) is applied before returning.
    """
    context_graph = load_variable_from_library('context_graph')

    k = context_graph.vid2ckey[vine_key]
    v = context_graph.task_dict[k]

    if context_graph.is_dts_key(k):
        result = compute_dts_key(context_graph, k, v)
    else:
        result = compute_sexpr_key(context_graph, k, v)

    context_graph.save_result_of_key(k, result)
    if not os.path.exists(context_graph.outfile_remote_name[k]):
        raise Exception(f"Output file {context_graph.outfile_remote_name[k]} does not exist after writing")
    if os.stat(context_graph.outfile_remote_name[k]).st_size == 0:
        raise Exception(f"Output file {context_graph.outfile_remote_name[k]} is empty after writing")

    time.sleep(context_graph.extra_sleep_time_of[k])

    return True
