import os, uuid, cloudpickle, types, time, random, hashlib, collections
from ndcctools.taskvine import cvine
from ndcctools.taskvine.dagvine import cdagvine
from ndcctools.taskvine.graph_definition import (
    GraphKeyResult, TaskGraph, compute_dts_key, compute_sexpr_key, 
    compute_single_key, hash_name, hashable
)
from ndcctools.taskvine.utils import load_variable_from_library


class Library:
    def __init__(self, py_manager, libname, libcores):
        self.py_manager = py_manager
        self.libname = libname
        assert self.libname is not None, "libname must be provided"

        self.libcores = libcores
        assert self.libcores is not None, "libcores must be provided"

        self.libtask = None

        # these modules are always included in the preamble of the library task, so that function calls can execute directly
        # using the loaded context without importing them over and over again
        self.hoisting_modules = [
            os, cloudpickle, GraphKeyResult, TaskGraph, uuid, hashlib, random, types, collections, time,
            load_variable_from_library, compute_dts_key, compute_sexpr_key, compute_single_key, hash_name, hashable
        ]

        # environment files serve as additional inputs to the library task, where each key is the local path and the value is the remote path
        # those local files will be sent remotely to the workers so tasks can access them as appropriate
        self.env_files = {}

        # context loader is a function that will be used to load the library context on remote nodes.
        self.context_loader_func = None
        self.context_loader_args = []
        self.context_loader_kwargs = {}

        self.local_path = None
        self.remote_path = None

    def add_hoisting_modules(self, new_modules):
        assert isinstance(new_modules, list), "new_modules must be a list of modules"
        self.hoisting_modules.extend(new_modules)

    def add_env_files(self, new_env_files):
        assert isinstance(new_env_files, dict), "new_env_files must be a dictionary"
        self.env_files.update(new_env_files)

    def set_context_loader(self, context_loader_func, context_loader_args=[], context_loader_kwargs={}):
        self.context_loader_func = context_loader_func
        self.context_loader_args = context_loader_args
        self.context_loader_kwargs = context_loader_kwargs

    def install(self):
        self.libtask = self.py_manager.create_library_from_functions(
            self.libname,
            compute_single_key,
            library_context_info=[self.context_loader_func, self.context_loader_args, self.context_loader_kwargs],
            add_env=False,
            function_infile_load_mode="json",
            hoisting_modules=self.hoisting_modules,
        )
        for local, remote in self.env_files.items():
            # check if the local file exists
            if not os.path.exists(local):
                raise FileNotFoundError(f"Local file {local} not found")
            # attach as the input file to the library task
            self.libtask.add_input(self.py_manager.declare_file(local, cache=True, peer_transfer=True), remote)
        self.libtask.set_cores(self.libcores)
        self.libtask.set_function_slots(self.libcores)
        self.py_manager.install_library(self.libtask)

    def uninstall(self):
        self.py_manager.remove_library(self.libname)
