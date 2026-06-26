# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os
import uuid
import cloudpickle
import types
import time
import random
import hashlib
import collections

from ..workflow import Workflow, TaskOutputRef, TaskOutputWrapper
from .execution import run_scheduler_keys
from ndcctools.taskvine.utils import load_variable_from_library


class TaskRunnerRegistration:
    def __init__(self, vine_graph):
        self.vine_graph = vine_graph

        self.name = None
        self.cores = None

        self.task = None

        # These modules are included in the generated function context so task calls can execute directly.
        self.hoisting_modules = [
            os, cloudpickle, Workflow, TaskOutputRef, TaskOutputWrapper, uuid, hashlib, random, types, collections, time,
            load_variable_from_library, run_scheduler_keys
        ]

        # Environment files are sent with the task runner context and exposed under their remote paths.
        self.env_files = {}

        # The context loader rebuilds the Workflow object in the remote function context.
        self.context_loader_func = None
        self.context_loader_args = []
        self.context_loader_kwargs = {}

        self.local_path = None
        self.remote_path = None

    def set_cores(self, cores):
        self.cores = cores

    def set_name(self, name):
        self.name = name

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
        assert self.name is not None, "Task runner name must be set before installing (use set_name method)"
        assert self.cores is not None, "Task runner cores must be set before installing (use set_cores method)"

        self.task = self.vine_graph.create_library_from_functions(
            self.name,
            run_scheduler_keys,
            library_context_info=[self.context_loader_func, self.context_loader_args, self.context_loader_kwargs],
            add_env=False,
            function_infile_load_mode="json",
            hoisting_modules=self.hoisting_modules,
        )
        for local, remote in self.env_files.items():
            if not os.path.exists(local):
                raise FileNotFoundError(f"Local file {local} not found")
            self.task.add_input(self.vine_graph.declare_file(local, cache=True, peer_transfer=True), remote)
        self.task.set_cores(self.cores)
        self.task.set_function_slots(self.cores)
        self.vine_graph.install_library(self.task)

    def uninstall(self):
        self.vine_graph.remove_library(self.name)
