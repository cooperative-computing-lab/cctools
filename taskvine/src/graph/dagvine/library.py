import os, uuid, cloudpickle, types, time, random, hashlib, collections
from ndcctools.taskvine import cvine
from ndcctools.taskvine.dagvine import cdagvine
from ndcctools.taskvine.graph_definition import (
    GraphKeyResult, TaskGraph, compute_dts_key, compute_sexpr_key, 
    compute_single_key, hash_name, hashable, init_task_graph_context
)
from ndcctools.taskvine.utils import load_variable_from_library


class Library:
    def __init__(self, hoisting_modules=[], env_files={}):
        self.libtask = None

        self.hoisting_modules = hoisting_modules
        self.env_files = env_files

        self.libcores = -1
        self.local_path = None
        self.remote_path = None

    def add_hoisting_modules(self, new_modules):
        assert isinstance(new_modules, list), "new_modules must be a list of modules"
        self.hoisting_modules.extend(new_modules)

    def add_env_files(self, new_env_files):
        assert isinstance(new_env_files, dict), "new_env_files must be a dictionary"
        self.env_files.update(new_env_files)

    def install(self, manager, libcores, vine_graph):
        self.libcores = libcores

        assert cdagvine.vine_task_graph_get_proxy_function_name(vine_graph) == compute_single_key.__name__

        self.local_path = f"library-task-graph-{uuid.uuid4()}.pkl"
        self.remote_path = self.local_path

        self.hoisting_modules += [
            os, cloudpickle, GraphKeyResult, TaskGraph, uuid, hashlib, random, types, collections, time,
            load_variable_from_library, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable
        ]
        lib_name = cdagvine.vine_task_graph_get_proxy_library_name(vine_graph)
        self.libtask = manager.create_library_from_functions(
            lib_name,
            compute_single_key,
            library_context_info=[init_task_graph_context, [], {"task_graph_path": self.remote_path}],
            add_env=False,
            function_infile_load_mode="json",
            hoisting_modules=self.hoisting_modules,
        )
        self.libtask.add_input(manager.declare_file(self.local_path), self.remote_path)
        for local, remote in self.env_files.items():
            self.libtask.add_input(manager.declare_file(local, cache=True, peer_transfer=True), remote)
        self.libtask.set_cores(self.libcores)
        self.libtask.set_function_slots(self.libcores)
        manager.install_library(self.libtask)