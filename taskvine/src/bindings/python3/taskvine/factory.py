# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
#

import atexit
import distutils.spawn
import errno
import json
import os
import shutil
import subprocess
import tempfile
import time


##
# \class Factory
# Launch a taskvine factory.
#
# The command line arguments for `vine_factory` can be set for a
# factory object (with dashes replaced with underscores). Creating a factory
# object does not immediately launch it, so this is a good time to configure
# the resources, number of workers, etc. Factory objects function as Python
# context managers, so to indicate that a set of commands should be run with
# a factory running, wrap them in a `with` statement. The factory will be
# cleaned up automatically at the end of the block. You can also make
# config changes to the factory while it is running. As an example,
#
#     # normal vine setup stuff
#     workers = taskvine.Factory("sge", "myproject")
#     workers.cores = 4
#     with workers:
#         # submit some tasks
#         workers.max_workers = 300
#         # got a pile of tasks, allow more workers
#     # any additional cleanup steps on the manager
class Factory(object):
    _command_line_options = [
        "amazon-config",
        "autosize",
        "batch-options",
        "batch-type",
        "capacity",
        "catalog",
        "condor-requirements",
        "config-file",
        "cores",
        "debug",
        "debug-file",
        "debug-file-size",
        "disk",
        "env",
        "extra-options",
        "factory-timeout",
        "foremen-name",
        "gpus",
        "k8s-image",
        "k8s-worker-image",
        "max-workers",
        "manager-name",
        "memory",
        "mesos-master",
        "mesos-path",
        "mesos-preload",
        "min-workers",
        "password",
        "python-env",
        "python-package",
        "run-factory-as-manager",
        "runos",
        "scratch-dir",
        "ssl",
        "tasks-per-worker",
        "timeout",
        "worker-binary",
        "workers-per-cycle",
        "wrapper",
        "wrapper-input",
    ]

    # subset of command line options that can be written to the configuration
    # file, and therefore they can be changed once the factory is running.
    _config_file_options = [
        "autosize",
        "capacity",
        "cores",
        "disk",
        "factory-timeout",
        "foremen-name",
        "manager-name",
        "max-workers",
        "memory",
        "min-workers",
        "tasks-per-worker",
        "timeout",
        "workers-per-cycle",
        "condor-requirements",
    ]

    ##
    # Create a factory for the given batch_type and manager name.
    #
    # One of `manager_name`, `manager_host_port`, or `manager` should be specified.
    # If factory_binary or worker_binary is not
    # specified, $PATH will be searched.
    def __init__(self, batch_type="local", manager=None, manager_host_port=None, manager_name=None, factory_binary=None, worker_binary=None, log_file=os.devnull):
        self._config_file = None
        self._factory_proc = None
        self._log_file = log_file
        self._error_file = None
        self._scratch_safe_to_delete = False

        self._opts = {}

        self._set_manager(batch_type, manager, manager_host_port, manager_name)

        self._opts["batch-type"] = batch_type
        self._opts["worker-binary"] = self._find_exe(worker_binary, "vine_worker")
        self._factory_binary = self._find_exe(factory_binary, "vine_factory")

        self._opts["scratch-dir"] = None
        if manager:
            self._opts["scratch-dir"] = manager.staging_directory

    def _set_manager(self, batch_type, manager, manager_host_port, manager_name):
        if not (manager or manager_host_port or manager_name):
            raise ValueError("Either manager, manager_host_port, or manager_name or manager should be specified.")

        if manager_name:
            self._opts["manager-name"] = manager_name

        if manager:
            if batch_type == "local":
                manager_host_port = f"localhost:{manager.port}"
            elif manager.name:
                self._opts["manager-name"] = manager_name

            if manager.using_ssl:
                self._opts["ssl"] = True

        if manager_host_port:
            try:
                (host, port) = [x for x in manager_host_port.split(":") if x]
                self._opts["manager-host"] = host
                self._opts["manager-port"] = port
                return
            except (TypeError, ValueError):
                raise ValueError("manager_host_port is not of the form HOST:PORT")

    def _find_exe(self, path, default):
        if path is None:
            out = distutils.spawn.find_executable(default)
        else:
            out = path
        if out is None or not os.access(out, os.F_OK):
            raise OSError(errno.ENOENT, "Command not found", out or default)
        if not os.access(out, os.X_OK):
            raise OSError(errno.EPERM, os.strerror(errno.EPERM), out)
        return os.path.abspath(out)

    def __getattr__(self, name):
        if name[0] == "_":
            # For names that start with '_', immediately return the attribute.
            # If the name does not start with '_' we assume is a factory option.
            return object.__getattribute__(self, name)

        # original command line options use - instead of _. _ is required by
        # the naming conventions of python (otherwise - is taken as 'minus')
        name_with_hyphens = name.replace("_", "-")

        if name_with_hyphens in Factory._command_line_options:
            try:
                return object.__getattribute__(self, "_opts")[name_with_hyphens]
            except KeyError:
                raise KeyError("{} is a valid factory attribute, but has not been set yet.".format(name))
        else:
            raise AttributeError("{} is not a supported option".format(name))

    def __setattr__(self, name, value):
        # original command line options use - instead of _. _ is required by
        # the naming conventions of python (otherwise - is taken as 'minus')
        name_with_hyphens = name.replace("_", "-")

        if name[0] == "_":
            # For names that start with '_', immediately set the attribute.
            # If the name does not start with '_' we assume is a factory option.
            object.__setattr__(self, name, value)
        elif self._factory_proc:
            # if factory is already running, only accept attributes that can
            # changed dynamically
            if name_with_hyphens in Factory._config_file_options:
                self._opts[name_with_hyphens] = value
                self._write_config()
            elif name_with_hyphens in Factory._command_line_options:
                raise AttributeError("{} cannot be changed once the factory is running.".format(name))
            else:
                raise AttributeError("{} is not a supported option".format(name))
        else:
            if name_with_hyphens in Factory._command_line_options:
                self._opts[name_with_hyphens] = value
            else:
                raise AttributeError("{} is not a supported option".format(name))

    def _construct_command_line(self):
        # check for environment file
        args = [self._factory_binary]

        args += ["--parent-death"]
        args += ["--config-file", self._config_file]

        if self._opts["batch-type"] == "local":
            self._opts["extra-options"] = self._opts.get("extra-options", "") + " --parent-death"

        for opt in self._opts:
            if opt not in Factory._command_line_options:
                continue
            if opt in Factory._config_file_options:
                continue
            if self._opts[opt] is True:
                args.append("--{}".format(opt))
            else:
                args.append("--{}={}".format(opt, self._opts[opt]))

        if "manager-host" in self._opts:
            args += [self._opts["manager-host"], self._opts["manager-port"]]

        return args

    ##
    # Start a factory process.
    #
    # It's best to use a context manager (`with` statement) to automatically
    # handle factory startup and tear-down. If another mechanism will ensure
    # cleanup (e.g. running inside a container), manually starting the factory
    # may be useful to provision workers from inside a Jupyter notebook.
    def start(self):
        if self._factory_proc is not None:
            # if factory already running, just update its config
            self._write_config()
            return

        if not self.scratch_dir:
            candidate = os.getcwd()
            if candidate.startswith("/afs") and self.batch_type == "condor":
                candidate = os.environ.get("TMPDIR", "/tmp")
            candidate = os.path.join(candidate, f"vine-factory-{os.getuid()}")
            if not os.path.exists(candidate):
                os.makedirs(candidate)
            self.scratch_dir = candidate

        # specialize scratch_dir for this run
        self.scratch_dir = tempfile.mkdtemp(prefix="vine-factory-", dir=self.scratch_dir)
        self._scratch_safe_to_delete = True

        atexit.register(lambda: os.path.exists(self.scratch_dir) and shutil.rmtree(self.scratch_dir))

        self._error_file = os.path.join(self.scratch_dir, "error.log")
        self._config_file = os.path.join(self.scratch_dir, "config.json")

        self._write_config()
        logfd = open(self._log_file, "a")
        errfd = open(self._error_file, "w")
        devnull = open(os.devnull, "w")
        self._factory_proc = subprocess.Popen(self._construct_command_line(), stdin=devnull, stdout=logfd, stderr=errfd)
        devnull.close()
        logfd.close()
        errfd.close()

        # ugly... give factory time to read configuration file
        time.sleep(1)

        status = self._factory_proc.poll()
        if status:
            with open(self._error_file) as error_f:
                error_log = error_f.read()
                raise RuntimeError("Could not execute vine_factory. Exited with status: {}\n{}".format(str(status), error_log))
        return self

    ##
    # Stop the factory process.
    def stop(self):
        if self._factory_proc is None:
            raise RuntimeError("Factory not yet started")
        self._factory_proc.terminate()
        self._factory_proc.wait()
        self._factory_proc = None
        self._config_file = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def __del__(self):
        if self._factory_proc is not None:
            self.stop()

        if shutil and self._scratch_safe_to_delete and self.scratch_dir and os.path.exists(self.scratch_dir):
            shutil.rmtree(self.scratch_dir)

    def _write_config(self):
        if self._config_file is None:
            return

        opts_subset = dict([(opt, self._opts[opt]) for opt in self._opts if opt in Factory._config_file_options])
        with open(self._config_file, "w") as f:
            json.dump(opts_subset, f, indent=4)

    def set_environment(self, env):
        self._env_file = env
