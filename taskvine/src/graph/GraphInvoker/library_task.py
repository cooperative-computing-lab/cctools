import os, uuid, cloudpickle, types, time, random, hashlib, collections
from ndcctools.taskvine import cvine
from ndcctools.taskvine.graph_definition import (
    GraphKeyResult, TaskGraph, compute_dts_key, compute_sexpr_key, 
    compute_single_key, hash_name, hashable, init_task_graph_context
)
from ndcctools.taskvine.utils import load_variable_from_library


class LibraryTask:
    def __init__(self, libcores=16, hoisting_modules=[], env_files={}):
        self._libtask = None

        self.libcores = libcores
        self.hoisting_modules = hoisting_modules
        self.env_files = env_files

        self.local_path = None
        self.remote_path = None

    def add_hoisting_modules(self, new_modules):
        assert isinstance(new_modules, list), "new_modules must be a list of modules"
        self.hoisting_modules.extend(new_modules)

    def add_env_files(self, new_env_files):
        assert isinstance(new_env_files, dict), "new_env_files must be a dictionary"
        self.env_files.update(new_env_files)

    def install(self, manager, vine_graph):
        assert cvine.vine_task_graph_get_proxy_function_name(vine_graph) == compute_single_key.__name__

        self.local_path = f"library-task-graph-{uuid.uuid4()}.pkl"
        self.remote_path = self.local_path

        self.hoisting_modules += [
            os, cloudpickle, GraphKeyResult, TaskGraph, uuid, hashlib, random, types, collections, time,
            load_variable_from_library, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable
        ]
        lib_name = cvine.vine_task_graph_get_proxy_library_name(vine_graph)
        self._libtask = manager.create_library_from_functions(
            lib_name,
            compute_single_key,
            library_context_info=[init_task_graph_context, [], {"task_graph_path": self.remote_path}],
            add_env=False,
            function_infile_load_mode="json",
            hoisting_modules=self.hoisting_modules,
        )
        self._libtask.add_input(manager.declare_file(self.local_path), self.remote_path)
        for local, remote in self.env_files.items():
            self._libtask.add_input(manager.declare_file(local, cache=True, peer_transfer=True), remote)
        self._libtask.set_cores(self.libcores)
        self._libtask.set_function_slots(self.libcores)
        manager.install_library(self._libtask)