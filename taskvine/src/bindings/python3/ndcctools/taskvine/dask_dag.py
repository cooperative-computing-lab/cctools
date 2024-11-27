# Copyright (C) 2023- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import time
import types
import dask
import json
import hashlib
import cloudpickle
from uuid import uuid4
from collections import defaultdict
from dask.utils import ensure_dict
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer


def hash_name(*args):
    out_str = ""
    for arg in args:
        out_str += str(arg)
    return hashlib.sha256(out_str.encode('utf-8')).hexdigest()[:12]

def convert_args(dsk_key, dsk, args, blockwise_args):
    try:
        if args in dsk:
            return hash_name(dsk_key, args)
    except:
        pass
    if isinstance(args, list):
        return [convert_args(dsk_key, dsk, item, blockwise_args) for item in args]
    elif isinstance(args, tuple):
        # nested tuple is not allowed
        return tuple(convert_args(dsk_key, dsk, item, blockwise_args) for item in args)
    else:
        if isinstance(args, str) and args.startswith('__dask_blockwise__'):
            blockwise_arg_idx = int(args.split('__')[-1])
            return blockwise_args[blockwise_arg_idx]
        return args

def save_to_json(data, filename):
    def map_keys_to_str(d, hlg):
        if isinstance(d, dict):
            return {str(k): map_keys_to_str(v, hlg) for k, v in d.items()}
        elif isinstance(d, list):
            return [map_keys_to_str(i, hlg) for i in d]
        elif isinstance(d, tuple):
            try:
                if d in hlg:
                    return str(d)
            except:
                # in case there is list type nested in tuple
                pass
            return tuple(map_keys_to_str(i, hlg) for i in d)
        else:
            return d
    def customer_serializer(obj):
        try:
            return str(obj)
        except Exception:
            print(f"ERROR: Unexpected type: {type(obj)}")
            exit(1)
    data_with_str_keys = map_keys_to_str(data, data)
    with open(filename, "w") as f:
        json.dump(data_with_str_keys, f, indent=4, default=customer_serializer)


class DaskVineDag:
    """A directed graph that encodes the steps and state a computation needs.
    Single computations are encoded as s-expressions, therefore it is 'upside-down',
    in the sense that the children of a node are the nodes required to compute it.
    E.g., for

    dsk = {'x': 1,
           'y': 2,
           'z': (add, 'x', 'y'),
           'w': (sum, ['x', 'y', 'z']),
           'v': [(sum, ['w', 'z']), 2]
           }

    'z' has as children 'x' and 'y'.

    Each node is referenced by its key. When the value of a key is list of
    sexprs, like 'v' above, and low_memory_mode is True, then a key is automatically computed recursively
    for each computation.

    Computation is done lazily. The DaskVineDag is initialized from a task graph, but not
    computation is decoded. To use the DaskVineDag:
        - DaskVineDag.set_targets(keys): Request the computation associated with key to be decoded.
        - DaskVineDag.get_ready(): A list of [key, sexpr] of expressions that are ready
          to be executed.
        - DaskVineDag.set_result(key, value): Sets the result of key to value.
        - DaskVineDag.get_result(key): Get result associated with key. Raises DagNoResult
        - DaskVineDag.has_result(key): Whether the key has a computed result. """

    @staticmethod
    def keyp(s):
        return DaskVineDag.hashable(s) and not DaskVineDag.taskp(s)

    @staticmethod
    def taskp(s):
        return isinstance(s, tuple) and len(s) > 0 and callable(s[0])

    @staticmethod
    def listp(s):
        return isinstance(s, list)

    @staticmethod
    def symbolp(s):
        return not (DaskVineDag.taskp(s) or DaskVineDag.listp(s))

    @staticmethod
    def hashable(s):
        try:
            hash(s)
            return True
        except TypeError:
            return False

    def __init__(self, dsk, keys, low_memory_mode=False, expand_hlg=False, save_expanded_hlg_dir=None):
        self._keys = keys

        if expand_hlg:
            self._dsk = self._expand_hlg(dsk, save_dir=save_expanded_hlg_dir)
        else:
            self._dsk = dsk

        # child -> parents. I.e., which parents needs the result of child
        self._parents_of = defaultdict(lambda: set())

        # parent->children still waiting for result. A key is ready to be computed when children left is []
        self._missing_of = {}

        # parent->nchildren get the number of children for parent computation
        self._children_of = {}

        # key->value of its computation
        self._result_of = {}

        # child -> nodes that use the child as an input, and that have not been completed
        self._pending_parents_of = defaultdict(lambda: set())

        # key->depth. The shallowest level the key is found
        self._depth_of = defaultdict(lambda: float('inf'))

        # target keys that the dag should compute
        self._targets = set()

        self._working_graph = dict(self._dsk)
        if low_memory_mode:
            self._flatten_graph()

        self.initialize_graph()

    def _expand_hlg(self, dsk, save_dir=None):
    
        print(f"expanding {len(dsk)} tasks in the original hlg......")
        time_start = time.time()
        task_dict = {}

        for k, sexpr in dsk.items():
            callable = sexpr[0]
            args = sexpr[1:]

            if isinstance(callable, dask.optimization.SubgraphCallable):
                task_dict[k] = hash_name(k, callable.outkey)
                for sub_key, sub_sexpr in callable.dsk.items():
                    unique_key = hash_name(k, sub_key)
                    task_dict[unique_key] = convert_args(k, callable.dsk, sub_sexpr, args)
            elif isinstance(callable, types.FunctionType):
                task_dict[k] = sexpr
            else:
                print(f"ERROR: unexpected type: {type(callable)}")
                exit(1)

        layers = ensure_dict({'layer': task_dict})
        dependencies = {'layer': set()}
        hlg = HighLevelGraph(layers, dependencies)

        if save_dir:
            assert os.path.exists(save_dir)
            with open(os.path.join(save_dir, 'expanded_hlg.pkl'), 'wb') as f:
                cloudpickle.dump(hlg, f)
            with open(os.path.join(save_dir, 'keys.pkl'), 'wb') as f:
                cloudpickle.dump(self._keys, f)

            save_to_json(task_dict, os.path.join(save_dir, 'expanded_hlg.json'))

        print(f"expanding hlg done, now the dag has {len(hlg)} tasks, time taken: {round(time.time() - time_start, 6)}s")

        return hlg

    def left_to_compute(self):
        return len(self._working_graph) - len(self._result_of)

    def graph_keyp(self, s):
        if DaskVineDag.keyp(s):
            return s in self._working_graph
        return False

    def depth_of(self, key):
        return self._depth_of[key]

    def initialize_graph(self):
        for key, sexpr in self._working_graph.items():
            self.set_relations(key, sexpr)

    def find_dependencies(self, sexpr, depth=0):
        dependencies = set()
        if self.graph_keyp(sexpr):
            dependencies.add(sexpr)
            self._depth_of[sexpr] = min(depth, self._depth_of[sexpr])
        elif not DaskVineDag.symbolp(sexpr):
            for sub in sexpr:
                dependencies.update(self.find_dependencies(sub, depth + 1))
        return dependencies

    def set_relations(self, key, sexpr):
        sexpr = self._working_graph[key]
        self._children_of[key] = self.find_dependencies(sexpr)
        self._missing_of[key] = set(self._children_of[key])

        for c in self._children_of[key]:
            self._parents_of[c].add(key)
            self._pending_parents_of[c].add(key)

    def get_ready(self):
        """ List of [(key, sexpr),...] ready for computation.
        This call should be used only for
        bootstrapping. Further calls should use DaskVineDag.set_result to discover
        the new computations that become ready to be executed. """
        rs = {}
        for (key, cs) in self._missing_of.items():
            if self.has_result(key) or cs:
                continue
            sexpr = self._working_graph[key]
            if self.graph_keyp(sexpr):
                rs.update(self.set_result(key, self.get_result(sexpr)))
            elif self.symbolp(sexpr):
                rs.update(self.set_result(key, sexpr))
            else:
                rs[key] = (key, sexpr)
        return rs.values()

    def set_result(self, key, value):
        """ Sets new result and propagates in the DaskVineDag. Returns a list of [(key, sexpr),...]
        of computations that become ready to be executed """
        rs = {}
        self._result_of[key] = value

        for p in self._parents_of[key]:
            self._missing_of[p].discard(key)

            if self._missing_of[p]:
                continue

            sexpr = self._working_graph[p]
            rs[p] = (p, sexpr)

        for c in self._children_of[key]:
            self._pending_parents_of[c].discard(key)

        return rs.values()

    def _flatten_graph(self):
        """ Recursively decomposes a sexpr associated with key, so that its arguments, if any
        are keys. """
        for key in list(self._working_graph.keys()):
            self.flatten_rec(key, self._working_graph[key], toplevel=True)

    def _add_second_targets(self, key):
        v = self._working_graph[key]
        if self.graph_keyp(v):
            lst = [v]
        elif DaskVineDag.listp(v):
            lst = v
        else:
            return
        for c in lst:
            if self.graph_keyp(c):
                self._targets.add(c)
                self._add_second_targets(c)

    def flatten_rec(self, key, sexpr, toplevel=False):
        if key in self._working_graph and not toplevel:
            return
        if DaskVineDag.symbolp(sexpr):
            return

        nargs = []
        next_flat = []
        cons = type(sexpr)

        for arg in sexpr:
            if DaskVineDag.symbolp(arg):
                nargs.append(arg)
            else:
                next_key = uuid4()
                nargs.append(next_key)
                next_flat.append((next_key, arg))

        self._working_graph[key] = cons(nargs)
        for (n, a) in next_flat:
            self.flatten_rec(n, a)

    def has_result(self, key):
        return key in self._result_of

    def get_result(self, key):
        try:
            return self._result_of[key]
        except KeyError:
            raise DaskVineNoResult(key)

    def get_children(self, key):
        return self._children_of[key]

    def get_missing_children(self, key):
        return self._missing_of[key]

    def get_parents(self, key):
        return self._parents_of[key]

    def get_pending_parents(self, key):
        return self._pending_parents_of[key]

    def set_targets(self, keys):
        """ Values of keys that need to be computed. """
        self._targets.update(keys)
        for k in keys:
            self._add_second_targets(k)
        return self.get_ready()

    def get_targets(self):
        return self._targets


class DaskVineNoResult(Exception):
    """Exception raised when asking for a result from a computation that has not been performed."""
    pass
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
