##
# @package taskvine
#
# Python API for the TaskVine workflow framework.
#
# TaskVine is a framework for building large scale distributed data intensive
# applications that run on clusters, clouds, grids, and similar distributed systems.
# A TaskVine application consists of a main program that creates a @ref Manager object,
# and then submits @ref Task objects that use @ref File objects representing data sources.
# The manager distributes tasks across available workers and returns results to
# the main application.
#
# See the <a href=http://cctools.readthedocs.io/en/latest/taskvine>TaskVine Manual</a> for complete documentation.
#
# - @ref Manager
# - @ref Task / @ref PythonTask / @ref FunctionCall
# - @ref File
# - @ref Factory
#
# The objects and methods provided by this package correspond closely
# to the native C API.

import itertools
import math
import copy
import os
import sys
import json
import errno
import tempfile
import subprocess
import distutils.spawn
import uuid
import textwrap
import shutil
import atexit
import time
from pathlib import Path

def set_port_range(low_port, high_port):
    if low_port > high_port:
        raise TypeError("low_port {} should be smaller than high_port {}".format(low_port, high_port))

    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)


##
# \class File
#
# TaskVine File Object
#
# The superclass of all TaskVine file types.
class File(object):
    def __init__(self, internal_file):
        self._file = internal_file

    def __bool__(self):
        # We need this because the len of some files is 0, which would evaluate
        # to false.
        return True

    ##
    # Return the contents of a file object as a string.
    # Typically used to return the contents of an output buffer.
    #
    # @param self       A file object.
    def contents(self):
        return vine_file_contents(self._file)

    ##
    # Return the size of a file object, in bytes.
    #
    # @param self       A file object.
    def __len__(self):
        return vine_file_size(self._file)


##
# \class Task
#
# TaskVine Task object
#
# This class is used to create a task specification to be submitted to a @ref taskvine::Manager.
class Task(object):
    ##
    # Create a new task specification.
    #
    # @param self       Reference to the current task object.
    # @param command    The shell command line to be exected by the task.
    # @param task_info  Optional dictionary containing specified task parameters.
    def __init__(self, command, **task_info):
        self._task = None

        if isinstance(command, dict):
            raise TypeError(f"{command} is not a str. Did you mean **{command}?")

        # python dels regular tasks when they go out of scope.
        # mini tasks are freed when the manager frees their associated file structure
        self._manager_will_free = False

        self._task = vine_task_create(command)
        if not self._task:
            raise Exception("Unable to create internal Task structure")

        attributes = [
            "coprocess", "scheduler", "tag", "category",
            "snapshot_file", "retries", "cores", "memory",
            "disk", "gpus", "priority", "time_end",
            "time_start", "time_max", "time_min", "monitor_output"
        ]

        for key in attributes:
            self._set_from_dict(task_info, key)

        if "features" in task_info:
            if isinstance(task_info["features"], str):
                self.add_feature(task_info["features"])
            elif isinstance(task_info["features"], list):
                for feature in task_info["features"]:
                    self.add_feature(feature)
            else:
                raise Exception("Unable to create internal Task structure")

        if "inputs" in task_info:
            for key, value in task_info["inputs"].items():
                if not isinstance(key, File):
                    raise TypeError(f"{key} is not a TaskVine file")
                if isinstance(value, dict):
                    parameters = value
                elif isinstance(value, str):
                    parameters = { "remote_name": value }
                else:
                    raise TypeError(f"{value} is not a str or dict")
                self.add_input(key, **parameters)

        if "outputs" in task_info:
            for key, value in task_info["outputs"].items():
                if not isinstance(key, File):
                    raise TypeError(f"{key} is not a TaskVine file")
                if isinstance(value, dict):
                    parameters = value
                elif isinstance(value, str):
                    parameters = { "remote_name": value }
                else:
                    raise TypeError(f"{value} is not a str or dict")
                print(f"self.add_output({key}, {parameters})")
                self.add_output(key, **parameters)

        if "env" in task_info:
            for key, value in task_info["env"].items():
                self.set_env_var(key, value)

    def __del__(self):
        try:
            if self._manager_will_free:
                return
            if self._task:
                vine_task_delete(self._task)
        except Exception:
            # ignore exceptions, in case task has been already collected
            pass

    def _set_from_dict(self, task_info, key):
        try:
            value = task_info[key]
            method = f"set_{key}"
            setter = getattr(self, method)
            setter(value)
        except KeyError:
            pass

    @staticmethod
    def _determine_mount_flags(watch=False, failure_only=False, success_only=False):
        flags = VINE_TRANSFER_ALWAYS
        if watch:
            flags |= VINE_WATCH
        if failure_only:
            flags |= VINE_FAILURE_ONLY
        if success_only:
            flags |= VINE_SUCCESS_ONLY
        return flags

    @staticmethod
    def _determine_file_flags(cache=False, peer_transfer=False):
        flags = VINE_CACHE_NEVER
        if cache is True or cache == "workflow":
            flags |= VINE_CACHE
        if cache == "always":
            flags |= VINE_CACHE_ALWAYS
        if not peer_transfer:
            flags |= VINE_PEER_NOSHARE
        return flags

    ##
    # Finalizes the task definition once the manager that will execute is run.
    # This function is run by the manager before registering the task for
    # execution.
    #
    # @param self 	Reference to the current python task object
    # @param manager Manager to which the task was submitted
    def submit_finalize(self, manager):
        pass

    ##
    # Return a copy of this task
    #
    def clone(self):
        """Return a (deep)copy this task that can also be submitted to the taskvine."""
        new = copy.copy(self)
        new._task = vine_task_clone(self._task)
        return new

    ##
    # Set the command to be executed by the task.
    #
    # @param self       Reference to the current task object.
    # @param command    The command to be executed.
    def set_command(self, command):
        return vine_task_set_command(self._task, command)

    ##
    # Set the coprocess at the worker that should execute the task's command.
    # This is not needed for regular tasks.
    #
    # @param self       Reference to the current task object.
    # @param coprocess  The name of the coprocess.
    def set_coprocess(self, coprocess):
        return vine_task_set_coprocess(self._task, coprocess)

    ##
    # Set the worker selection scheduler for task.
    #
    # @param self       Reference to the current task object.
    # @param scheduler  One of the following schedulers to use in assigning a
    #                   task to a worker. See @ref vine_schedule_t for
    #                   possible values.
    def set_scheduler(self, scheduler):
        return vine_task_set_scheduler(self._task, scheduler)

    ##
    # Attach a user defined logical name to the task.
    #
    # @param self       Reference to the current task object.
    # @param tag        The tag to attach to task.
    def set_tag(self, tag):
        return vine_task_set_tag(self._task, tag)

    ##
    # Label the task with the given category. It is expected that tasks with the
    # same category have similar resources requirements (e.g. to disconnect slow workers).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the category
    def set_category(self, name):
        return vine_task_set_category(self._task, name)

    ##
    # Label the task with the given user-defined feature. Tasks with the
    # feature will only run on workers that provide it (see worker's
    # --feature option).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the feature.
    def add_feature(self, name):
        return vine_task_add_feature(self._task, name)

    ##
    # Add any input object to a task.
    #
    # @param self          Reference to the current task object.
    # @param file          A file object of class @ref File, such as from @ref declare_file, @ref declare_buffer, @ref declare_url, etc.
    # @param remote_name   The name of the file at the execution site.
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs). Default is False.
    #
    # For example:
    # @code
    # >>> url = m.declare_url(http://somewhere.edu/data.tgz)
    # >>> f = m.declare_untar(url)
    # >>> task.add_input(f,"data")
    # @endcode
    def add_input(self, file, remote_name):
        # SWIG expects strings
        if not isinstance(remote_name, str):
            raise TypeError(f"remote_name {remote_name} is not a str")

        flags = Task._determine_mount_flags()
        return vine_task_add_input(self._task, file._file, remote_name, flags)

    ##
    # Add any output object to a task.
    #
    # @param self          Reference to the current task object.
    # @param file          A file object of class @ref File, such as from @ref declare_file, or @ref declare_buffer
    # @param remote_name   The name of the file at the execution site.
    # @param watch         Watch the output file and send back changes as the task runs.
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param success_only  Whether the file should be retrieved only when the task succeeds. Default is False.
    # @param failure_only  Whether the file should be retrieved only when the task fails (e.g., debug logs). Default is False.
    #
    # For example:
    # @code
    # >>> file = m.declare_file("output.txt")
    # >>> task.add_output(file,"out")
    # @endcode
    def add_output(self, file, remote_name, watch=False, failure_only=None, success_only=None):
        # SWIG expects strings
        if not isinstance(remote_name, str):
            raise TypeError(f"remote_name {remote_name} is not a str")

        flags = Task._determine_mount_flags(watch, failure_only, success_only)
        return vine_task_add_output(self._task, file._file, remote_name, flags)

    ##
    # When monitoring, indicates a json-encoded file that instructs the monitor
    # to take a snapshot of the task resources. Snapshots appear in the JSON
    # summary file of the task, under the key "snapshots". Snapshots are taken
    # on events on files described in the monitor_snapshot_file. The
    # monitor_snapshot_file is a json encoded file with the following format:
    #
    # @code
    #   {
    #       "FILENAME": {
    #           "from-start":boolean,
    #           "from-start-if-truncated":boolean,
    #           "delete-if-found":boolean,
    #           "events": [
    #               {
    #                   "label":"EVENT_NAME",
    #                   "on-create":boolean,
    #                   "on-truncate":boolean,
    #                   "pattern":"REGEXP",
    #                   "count":integer
    #               },
    #               {
    #                   "label":"EVENT_NAME",
    #                   ...
    #               }
    #           ]
    #       },
    #       "FILENAME": {
    #           ...
    #   }
    # @endcode
    #
    # All keys but "label" are optional:
    #
    #   from-start:boolean         If FILENAME exits when task starts running, process from line 1. Default: false, as the task may be appending to an already existing file.
    #   from-start-if-truncated    If FILENAME is truncated, process from line 1. Default: true, to account for log rotations.
    #   delete-if-found            Delete FILENAME when found. Default: false
    #
    #   events:
    #   label        Name that identifies the snapshot. Only alphanumeric, -,
    #                and _ characters are allowed.
    #   on-create    Take a snapshot every time the file is created. Default: false
    #   on-truncate  Take a snapshot when the file is truncated.    Default: false
    #   on-pattern   Take a snapshot when a line matches the regexp pattern.    Default: none
    #   count        Maximum number of snapshots for this label. Default: -1 (no limit)
    #
    # Exactly one of on-create, on-truncate, or on-pattern should be specified.
    #
    # Once a task has finished, the snapshots are available as:
    #
    # @code
    # for s in t.resources_measured.snapshots:
    #   print(s.memory)
    # @endcode
    #
    # For more information, consult the manual of the resource_monitor.
    #
    # @param self           Reference to the current task object.
    # @param filename       The name of the snapshot events specification
    def set_snapshot_file(self, filename):
        return vine_task_set_snapshot_file(self._task, filename)

    ##
    # Adds an execution environment to the task. The environment file specified
    # is expected to expand to a directory with a bin/run_in_env file that will wrap
    # the task command (e.g. a poncho or a starch file, or any other vine mini_task
    # that creates such a wrapper). If specified multiple times,
    # environments are nested in the order given (i.e. first added is the first applied).
    # @param t A task object.
    # @param f The environment file.
    def add_environment(self, f):
        return vine_task_add_environment(self._task, f._file)

    ##
    # Indicate the number of times the task should be retried. If 0 (the
    # default), the task is tried indefinitely. A task that did not succeed
    # after the given number of retries is returned with result
    # VINE_RESULT_MAX_RETRIES.
    def set_retries(self, max_retries):
        return vine_task_set_retries(self._task, max_retries)

    ##
    # Indicate the number of cores required by this task.
    def set_cores(self, cores):
        return vine_task_set_cores(self._task, cores)

    ##
    # Indicate the memory (in MB) required by this task.
    def set_memory(self, memory):
        return vine_task_set_memory(self._task, memory)

    ##
    # Indicate the disk space (in MB) required by this task.
    def set_disk(self, disk):
        return vine_task_set_disk(self._task, disk)

    ##
    # Indicate the number of GPUs required by this task.
    def set_gpus(self, gpus):
        return vine_task_set_gpus(self._task, gpus)

    ##
    # Indicate the the priority of this task (larger means better priority, default is 0).
    def set_priority(self, priority):
        return vine_task_set_priority(self._task, priority)

    # Indicate the maximum end time (absolute, in microseconds from the Epoch) of this task.
    # This is useful, for example, when the task uses certificates that expire.
    # If less than 1, or not specified, no limit is imposed.
    def set_time_end(self, useconds):
        return vine_task_set_time_end(self._task, int(useconds))

    # Indicate the minimum start time (absolute, in microseconds from the Epoch) of this task.
    # Task will only be submitted to workers after the specified time.
    # If less than 1, or not specified, no limit is imposed.
    def set_time_start(self, useconds):
        return vine_task_set_time_start(self._task, int(useconds))

    # Indicate the maximum running time (in seconds) for a task in a
    # worker (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def set_time_max(self, useconds):
        return vine_task_set_time_max(self._task, int(useconds))

    # Indicate the minimum running time (in seconds) for a task in a worker
    # (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def set_time_min(self, seconds):
        return vine_task_set_time_min(self._task, int(seconds))

    ##
    # Set this environment variable before running the task.
    # If value is None, then variable is unset.
    def set_env_var(self, name, value=None):
        return vine_task_set_env_var(self._task, name, value)

    ##
    # Set a name for the resource summary output directory from the monitor.
    def set_monitor_output(self, directory):
        return vine_task_set_monitor_output(self._task, directory)

    ##
    # Get the user-defined logical name for the task.
    #
    # @code
    # >>> print(t.tag)
    # @endcode
    @property
    def tag(self):
        return vine_task_get_tag(self._task)

    ##
    # Get the category name for the task.
    #
    # @code
    # >>> print(t.category)
    # @endcode
    @property
    def category(self):
        return vine_task_get_category(self._task)

    ##
    # Get the shell command executed by the task.
    # @code
    # >>> print(t.command)
    # @endcode
    @property
    def command(self):
        return vine_task_get_command(self._task)

    ##
    # Get the standard output of the task. Must be called only after the task
    # completes execution.
    # @code
    # >>> print(t.std_output)
    # @endcode
    @property
    def std_output(self):
        return vine_task_get_stdout(self._task)

    ##
    # Get the standard output of the task. (Same as t.std_output for regular
    # taskvine tasks) Must be called only after the task completes execution.
    # @code
    # >>> print(t.output)
    # @endcode
    @property
    def output(self):
        return vine_task_get_stdout(self._task)

    ##
    # Get the task id number. Must be called only after the task was submitted.
    # @code
    # >>> print(t.id)
    # @endcode
    @property
    def id(self):
        return vine_task_get_id(self._task)

    ##
    # Get the exit code of the command executed by the task. Must be called only
    # after the task completes execution.
    # @code
    # >>> print(t.return_status)
    # @endcode
    @property
    def exit_code(self):
        return vine_task_get_exit_code(self._task)

    ##
    # Get the result of the task as an integer code, such as successful, missing file, etc.
    # See @ref vine_result_t for possible values.  Must be called only
    # after the task completes execution.
    # @code
    # >>> print(t.result)
    # 0
    # @endcode
    @property
    def result(self):
        return vine_task_get_result(self._task)

    ##
    # Return a string that explains the result of a task.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.result_string)
    # 'success'
    # @endcode
    @property
    def result_string(self):
        result = vine_result_string(vine_task_get_result(self._task))
        return result.lower().replace("_", " ")

    ##
    # Return True if task executed and its command terminated normally.
    # If True, the exit code of the command can be retrieved with @ref
    # exit_code. If False, the error condition can be retrieved with @ref
    # result and @result_string.  It must be called only after the task
    # completes execution.
    # @code
    # >>> # completed tasks with a failed command execution:
    # >>> print(t.completed())
    # True
    # >>> print(t.exit_code)
    # 1
    # >>> # task with an error condition:
    # >>> print(t.completed())
    # False
    # >>> print(t.result_string)
    # max retries
    # @endcode
    def completed(self):
        return self.result == VINE_RESULT_SUCCESS

    ##
    # Return True if task executed successfully, (i.e. its command terminated
    # normally with exit code 0 and produced all its declared output files).
    # Differs from @ref completed in that the exit code of the command should
    # be zero.
    # It must be called only after the task completes execution.
    # @code
    # >>> # completed tasks with a failed command execution:
    # >>> print(t.completed())
    # True
    # >>> print(t.successful())
    # False
    # @endcode
    def successful(self):
        return self.completed() and self.exit_code == 0

    ##
    # Return various integer performance metrics about a completed task.
    # Must be called only after the task completes execution.
    #
    # Valid metric names:
    # - time_when_submitted
    # - time_when_done
    # - time_when_commit_start
    # - time_when_commit_end
    # - time_when_retrieval
    # - time_workers_execute_last
    # - time_workers_execute_all
    # - time_workers_execute_exhaustion
    # - time_workers_execute_failure
    # - bytes_received
    # - bytes_sent
    # - bytes_transferred
    #
    # @code
    # >>> print(t.get_metric("total_submissions")
    # @endcode
    @property
    def get_metric(self, name):
        return vine_task_get_metric(self._task, name)

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    #
    # @code
    # >>> print(t.host)
    # @endcode
    @property
    def addrport(self):
        return vine_task_get_addrport(self._task)

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    #
    # @code
    # >>> print(t.host)
    # @endcode
    @property
    def hostname(self):
        return vine_task_get_hostname(self._task)

    ##
    # Get the resources measured for the task execution if resource monitoring is enabled.
    # Must be called only after the task completes execution. Valid fields:
    #
    # start:                     microseconds at the start of execution
    #
    # end:                       microseconds at the end of execution
    #
    # wall_time:                 microseconds spent during execution
    #
    # cpu_time:                  user + system time of the execution
    #
    # cores:                     peak number of cores used
    #
    # cores_avg:                 number of cores computed as cpu_time/wall_time
    #
    # gpus:                      peak number of gpus used
    #
    # max_concurrent_processes:  the maximum number of processes running concurrently
    #
    # total_processes:           count of all of the processes created
    #
    # virtual_memory:            maximum virtual memory across all processes
    #
    # memory:                    maximum resident size across all processes
    #
    # swap_memory:               maximum swap usage across all processes
    #
    # bytes_read:                number of bytes read from disk
    #
    # bytes_written:             number of bytes written to disk
    #
    # bytes_received:            number of bytes read from the network
    #
    # bytes_sent:                number of bytes written to the network
    #
    # bandwidth:                 maximum network bits/s (average over one minute)
    #
    # total_files:               total maximum number of files and directories of all the working directories in the tree
    #
    # disk:                      size in MB of all working directories in the tree
    #
    # @code
    # >>> print(t.resources_measured.memory)
    # @endcode
    @property
    def resources_measured(self):
        if not self._task.resources_measured:
            return None

        return self._task.resources_measured

    ##
    # Get the resources the task exceeded. For valid field see @ref resources_measured.
    #
    @property
    def limits_exceeded(self):
        if not self._task.resources_measured:
            return None

        if not self._task.resources_measured.limits_exceeded:
            return None

        return self._task.resources_measured.limits_exceeded

    ##
    # Get the resources the task requested to run. For valid fields see
    # @ref resources_measured.
    #
    @property
    def resources_requested(self):
        if not self._task.resources_requested:
            return None
        return self._task.resources_requested

    ##
    # Get the resources allocated to the task in its latest attempt. For valid
    # fields see @ref resources_measured.
    #
    @property
    def resources_allocated(self):
        if not self._task.resources_allocated:
            return None
        return self._task.resources_allocated


##
# \class PythonTask
#
# TaskVine PythonTask object
#
# The class represents a Task specialized to execute remote Python code.
#

try:
    import dill

    pythontask_available = True
except Exception:
    # Note that the intended exception here is ModuleNotFoundError.
    # However, that type does not exist in Python 2
    pythontask_available = False

try:
    from poncho import package_serverize
    poncho_available = True
except Exception:
    poncho_available = False


class PythonTask(Task):
    ##
    # Creates a new python task
    #
    # @param self 	Reference to the current python task object
    # @param func	python function to be executed by task
    # @param args	arguments used in function to be executed by task
    # @param kwargs	keyword arguments used in function to be executed by task
    def __init__(self, func, *args, **kwargs):
        if not pythontask_available:
            raise RuntimeError("PythonTask is not available. The dill module is missing.")

        self._pp_run = None
        self._output_loaded = False
        self._output = None
        self._tmpdir = None

        self._id = str(uuid.uuid4())
        self._func_file = f"function_{self._id}.p"
        self._args_file = f"args_{self._id}.p"
        self._out_file = f"out_{self._id}.p"
        self._wrapper = f"pytask_wrapper_{self._id}.py"
        self._command = self._python_function_command()

        # we delay any PythonTask initialization until the task is submitted to
        # a manager. This is because we don't know the staging directory where
        # the task should write its files.
        self._fn_def = (func, args, kwargs)
        super(PythonTask, self).__init__(self._command)

    ##
    # Finalizes the task definition once the manager that will execute is run.
    # This function is run by the manager before registering the task for
    # execution.
    #
    # @param self 	Reference to the current python task object
    # @param manager Manager to which the task was submitted
    def submit_finalize(self, manager):
        self._tmpdir = tempfile.mkdtemp(dir=manager.staging_directory)
        self._serialize_python_function(*self._fn_def)
        self._fn_def = None  # avoid possible memory leak
        self._create_wrapper()
        self._add_IO_files(manager)

    ##
    # returns the result of a python task as a python variable
    #
    # @param self	reference to the current python task object
    @property
    def output(self):
        if not self._output_loaded:
            if self.successful():
                try:
                    with open(os.path.join(self._tmpdir, "out_{}.p".format(self._id)), "rb") as f:
                        self._output = dill.load(f)
                except Exception as e:
                    self._output = e
            else:
                self._output = PythonTaskNoResult()
                print(self.std_output)
            self._output_loaded = True
        return self._output

    def __del__(self):
        try:
            if self._tmpdir and os.path.exists(self._tmpdir):
                shutil.rmtree(self._tmpdir)
        except Exception as e:
            if sys:
                sys.stderr.write("could not delete {}: {}\n".format(self._tmpdir, e))

    def _serialize_python_function(self, func, args, kwargs):
        with open(os.path.join(self._tmpdir, self._func_file), "wb") as wf:
            dill.dump(func, wf, recurse=True)
        with open(os.path.join(self._tmpdir, self._args_file), "wb") as wf:
            dill.dump([args, kwargs], wf, recurse=True)

    def _python_function_command(self, remote_env_dir=None):
        if remote_env_dir:
            py_exec = "python"
        else:
            py_exec = f"python{sys.version_info[0]}"

        command = f"{py_exec} {self._wrapper} {self._func_file} {self._args_file} {self._out_file}"
        return command

    def _add_IO_files(self, manager):
        def add_files(method, *files):
            for name in files:
                source=os.path.join(self._tmpdir, name)
                f = manager.declare_file(source, cache=False)
                method(f, name)
        add_files(self.add_input, self._wrapper, self._func_file, self._args_file)
        add_files(self.add_output, self._out_file)

    ##
    # creates the wrapper script which will execute the function. pickles output.
    def _create_wrapper(self):
        with open(os.path.join(self._tmpdir, self._wrapper), "w") as f:
            f.write(
                textwrap.dedent(
                    """\
                try:
                    import sys
                    import dill
                except ImportError:
                    print("Could not execute PythonTask function because a needed module for taskvine was not available.")
                    raise

                (fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
                with open (fn , 'rb') as f:
                    exec_function = dill.load(f)
                with open(args, 'rb') as f:
                    args, kwargs = dill.load(f)
                try:
                    exec_out = exec_function(*args, **kwargs)

                except Exception as e:
                    exec_out = e

                with open(out, 'wb') as f:
                    dill.dump(exec_out, f)

                print(exec_out)"""
                )
            )


class PythonTaskNoResult(Exception):
    pass


##
# TaskVine Manager
#
# The manager class is the primary object for a TaskVine application.
# To build an application, create a Manager instance, then create
# @ref taskvine::Task objects and submit them with @ref taskvine::Manager::submit
# Call @ref taskvine::Manager::wait to wait for tasks to complete.
# Run one or more vine_workers to perform work on behalf of the manager object.


class Manager(object):
    ##
    # Create a new manager.
    #
    # @param self       Reference to the current manager object.
    # @param port       The port number to listen on. If zero, then a random port is chosen. A range of possible ports (low, hight) can be also specified instead of a single integer.
    # @param name       The project name to use.
    # @param shutdown   Automatically shutdown workers when manager is finished. Disabled by default.
    # @param run_info_path Directory to write log and staging files per run. If None, defaults to "vine-run-info"
    # @param ssl        A tuple of filenames (ssl_key, ssl_cert) in pem format, or True.
    #                   If not given, then TSL is not activated. If True, a self-signed temporary key and cert are generated.
    # @param status_display_interval Number of seconds between updates to the jupyter status display. None, or less than 1 disables it.
    #
    # @see vine_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=VINE_DEFAULT_PORT, name=None, shutdown=False, run_info_path="vine-run-info", ssl=None, status_display_interval=None):
        self._shutdown = shutdown
        self._taskvine = None
        self._stats = None
        self._stats_hierarchy = None
        self._task_table = {}
        self._library_table = {}
        self._info_widget = None
        self._using_ssl = False

        # if we were given a range ports, rather than a single port to try.
        lower, upper = None, None
        try:
            lower, upper = port
            set_port_range(lower, upper)
            port = 0
        except TypeError:
            # if not a range, ignore
            pass
        except ValueError:
            raise ValueError("port should be a single integer, or a sequence of two integers")

        if status_display_interval and status_display_interval >= 1:
            self._info_widget = JupyterDisplay(interval=status_display_interval)

        try:
            if run_info_path:
                self.set_runtime_info_path(run_info_path)

            self._stats = vine_stats()
            self._stats_hierarchy = vine_stats()

            ssl_key, ssl_cert = self._setup_ssl(ssl, run_info_path)
            self._taskvine = vine_ssl_create(port, ssl_key, ssl_cert)

            if ssl_key:
                self._using_ssl = True

            if not self._taskvine:
                raise Exception("Could not create manager on port {}".format(port))

            if name:
                vine_set_name(self._taskvine, name)

            self._update_status_display()
        except Exception:
            sys.stderr.write("Unable to create internal taskvine structure.")
            raise


    def _free_manager(self):
        try:
            if self._taskvine:
                if self._shutdown:
                    self.shutdown_workers(0)
                self._update_status_display(force=True)
                vine_delete(self._taskvine)
                self._taskvine = None
        except Exception:
            # ignore exceptions, as we are going away...
            pass

    def __del__(self):
        self._update_status_display(force=True)
        self._free_manager()

    def _setup_ssl(self, ssl, run_info_path):
        if not ssl:
            return (None, None)

        if ssl is not True:
            return ssl

        (tmp, key) = tempfile.mkstemp(dir=run_info_path, prefix="key")
        os.close(tmp)
        (tmp, cert) = tempfile.mkstemp(dir=run_info_path, prefix="cert")
        os.close(tmp)

        atexit.register(lambda: os.path.exists(key) and os.unlink(key))
        atexit.register(lambda: os.path.exists(cert) and os.unlink(cert))

        cmd = f"openssl req -x509 -newkey rsa:4096 -keyout {key} -out {cert} -sha256 -days 365 -nodes -batch".split()

        output = ""
        try:
            output = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError as e:
            print(f"could not create temporary SSL key and cert {e}.\n{output}")
            raise e
        return (key, cert)

    def _update_status_display(self, force=False):
        try:
            if self._info_widget and self._info_widget.active():
                self._info_widget.update(self, force)
        except Exception as e:
            # no exception should cause the queue to fail
            print(f"status display error: {e}", file=sys.stderr)

    ##
    # Get the project name of the manager.
    # @code
    # >>> print(q.name)
    # @endcode
    @property
    def name(self):
        return vine_get_name(self._taskvine)

    ##
    # Get the listening port of the manager.
    # @code
    # >>> print(q.port)
    # @endcode
    @property
    def port(self):
        return vine_port(self._taskvine)

    ##
    # Whether the manager is using ssl to talk to workers
    # @code
    # >>> print(q.using_ssl)
    # @endcode
    @property
    def using_ssl(self):
        return self._using_ssl

    ##
    # Get the staging directory of the manager
    @property
    def logging_directory(self):
        return vine_get_runtime_path_log(self._taskvine, None)

    ##
    # Get the staging directory of the manager
    @property
    def staging_directory(self):
        return vine_get_runtime_path_staging(self._taskvine, None)

    ##
    # Get the caching directory of the manager
    @property
    def cache_directory(self):
        return vine_get_runtime_path_caching(self._taskvine, None)

    ##
    # Get manager statistics.
    # @code
    # >>> print(q.stats)
    # @endcode
    # The fields in @ref stats can also be individually accessed through this call. For example:
    # @code
    # >>> print(q.stats.workers_busy)
    # @endcode
    @property
    def stats(self):
        vine_get_stats(self._taskvine, self._stats)
        return self._stats

    ##
    # Get the task statistics for the given category.
    #
    # @param self   Reference to the current manager object.
    # @param category   A category name.
    # For example:
    # @code
    # s = q.stats_category("my_category")
    # >>> print(s)
    # @endcode
    # The fields in @ref vine_stats can also be individually accessed through this call. For example:
    # @code
    # >>> print(s.tasks_waiting)
    # @endcode
    def stats_category(self, category):
        stats = vine_stats()
        vine_get_stats_category(self._taskvine, category, stats)
        return stats

    ##
    # Get manager information as list of dictionaries
    # @param self Reference to the current manager object
    # @param request One of: "manager", "tasks", "workers", or "categories"
    # For example:
    # @code
    # import json
    # tasks_info = q.status("tasks")
    # @endcode
    def status(self, request):
        info_raw = vine_get_status(self._taskvine, request)
        info_json = json.loads(info_raw)
        del info_raw
        return info_json

    ##
    # Get resource statistics of workers connected.
    #
    # @param self 	Reference to the current manager object.
    # @return A list of dictionaries that indicate how many .workers
    # connected with a certain number of .cores, .memory, and disk.
    # For example:
    # @code
    # workers = q.summarize_workers()
    # >>> for w in workers:
    # >>>    print("{} workers with: {} cores, {} MB memory, {} MB disk".format(w.workers, w.cores, w.memory, w.disk)
    # @endcode
    def summarize_workers(self):
        from_c = vine_summarize_workers(self._taskvine)

        count = 0
        workers = []
        while True:
            s = rmsummayArray_getitem(from_c, count)
            if not s:
                break
            workers.append({"workers": int(s.workers), "cores": int(s.cores), "gpus": int(s.gpus), "memory": int(s.memory), "disk": int(s.disk)})
            rmsummary_delete(s)
            count += 1
        delete_rmsummayArray(from_c)
        return workers

    ##
    # Turn on or off first-allocation labeling for a given category. By
    # default, only cores, memory, and disk are labeled, and gpus are unlabeled.
    # NOTE: autolabeling is only meaningfull when task monitoring is enabled
    # (@ref enable_monitoring). When monitoring is enabled and a task exhausts
    # resources in a worker, mode dictates how taskvine handles the
    # exhaustion:
    # @param self Reference to the current manager object.
    # @param category A category name. If None, sets the mode by default for
    # newly created categories.
    # @param mode One of:
    #                  - VINE_ALLOCATION_MODE_FIXED Task fails (default).
    #                  - VINE_ALLOCATION_MODE_MAX If maximum values are
    #                  specified for cores, memory, disk, and gpus (e.g. via @ref
    #                  set_category_resources_max or @ref Task.set_memory),
    #                  and one of those resources is exceeded, the task fails.
    #                  Otherwise it is retried until a large enough worker
    #                  connects to the manager, using the maximum values
    #                  specified, and the maximum values so far seen for
    #                  resources not specified. Use @ref Task.set_retries to
    #                  set a limit on the number of times manager attemps
    #                  to complete the task.
    #                  - VINE_ALLOCATION_MODE_MIN_WASTE As above, but
    #                  manager tries allocations to minimize resource waste.
    #                  - VINE_ALLOCATION_MODE_MAX_THROUGHPUT As above, but
    #                  manager tries allocations to maximize throughput.
    def set_category_mode(self, category, mode):
        return vine_set_category_mode(self._taskvine, category, mode)

    ##
    # Turn on or off first-allocation labeling for a given category and
    # resource. This function should be use to fine-tune the defaults from @ref
    # set_category_mode.
    # @param self   Reference to the current manager object.
    # @param category A category name.
    # @param resource A resource name.
    # @param autolabel True/False for on/off.
    # @returns 1 if resource is valid, 0 otherwise.
    def set_category_autolabel_resource(self, category, resource, autolabel):
        return vine_enable_category_resource(self._taskvine, category, category, resource, autolabel)

    ##
    # Get current task state. See @ref vine_task_state_t for possible values.
    # @code
    # >>> print(q.task_state(task_id))
    # @endcode
    def task_state(self, task_id):
        return vine_task_state(self._taskvine, task_id)

    ##
    ## Enables resource monitoring for tasks. The resources measured are
    # available in the resources_measured member of the respective vine_task.
    # @param self   Reference to the current manager object.
    # @param watchdog If not 0, kill tasks that exhaust declared resources.
    # @param time_series If not 0, generate a time series of resources per task
    # in VINE_RUNTIME_INFO_DIR/vine-logs/time-series/ (WARNING: for long running
    # tasks these files may reach gigabyte sizes. This function is mostly used
    # for debugging.)
    #
    # Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self   Reference to the current manager object.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring(self, watchdog=True, time_series=False):
        return vine_enable_monitoring(self._taskvine, watchdog, time_series)

    ##
    # As @ref enable_monitoring, but it also generates a time series and a
    # debug file.
    # WARNING: Such files may reach gigabyte sizes for long running tasks.
    #
    # Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self   Reference to the current manager object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring_full(self, dirname=None, watchdog=True):
        return vine_enable_monitoring_full(self._taskvine, dirname, watchdog)

    ##
    # Enable P2P worker transfer functionality. Off by default
    #
    # @param self Reference to the current manager object.
    def enable_peer_transfers(self):
        return vine_enable_peer_transfers(self._taskvine)

    ##
    # Enable disconnect slow workers functionality for a given manager for tasks in
    # the "default" category, and for task which category does not set an
    # explicit multiplier.
    #
    # @param self       Reference to the current manager object.
    # @param multiplier The multiplier of the average task time at which point to disconnect a worker; if less than 1, it is disabled (default).
    def enable_disconnect_slow_workers(self, multiplier):
        return vine_enable_disconnect_slow_workers(self._taskvine, multiplier)

    ##
    # Enable disconnect slow workers functionality for a given manager.
    #
    # @param self       Reference to the current manager object.
    # @param name       Name of the category.
    # @param multiplier The multiplier of the average task time at which point to disconnect a worker; disabled if less than one (see @ref enable_disconnect_slow_workers)
    def enable_disconnect_slow_workers_category(self, name, multiplier):
        return vine_enable_disconnect_slow_workers_category(self._taskvine, name, multiplier)

    ##
    # Turn on or off draining mode for workers at hostname.
    #
    # @param self       Reference to the current manager object.
    # @param hostname   The hostname the host running the workers.
    # @param drain_mode If True, no new tasks are dispatched to workers at hostname, and empty workers are shutdown. Else, workers works as usual.
    def set_draining_by_hostname(self, hostname, drain_mode=True):
        return vine_set_draining_by_hostname(self._taskvine, hostname, drain_mode)

    ##
    # Determine whether there are any known tasks managerd, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current manager object.
    def empty(self):
        return vine_empty(self._taskvine)

    ##
    # Determine whether the manager can support more tasks.
    #
    # Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
    #
    # @param self       Reference to the current manager object.
    def hungry(self):
        return vine_hungry(self._taskvine)

    ##
    # Set the worker selection scheduler for manager.
    #
    # @param self       Reference to the current manager object.
    # @param scheduler  One of the following schedulers to use in assigning a
    #                   task to a worker. See @ref vine_schedule_t for
    #                   possible values.
    def set_scheduler(self, scheduler):
        return vine_set_scheduler(self._taskvine, scheduler)

    ##
    # Change the project name for the given manager.
    #
    # @param self   Reference to the current manager object.
    # @param name   The new project name.
    def set_name(self, name):
        return vine_set_name(self._taskvine, name)

    ##
    # Set the preference for using hostname over IP address to connect.
    # 'by_ip' uses IP addresses from the network interfaces of the manager
    # (standard behavior), 'by_hostname' to use the hostname at the manager, or
    # 'by_apparent_ip' to use the address of the manager as seen by the catalog
    # server.
    #
    # @param self Reference to the current manager object.
    # @param mode An string to indicate using 'by_ip', 'by_hostname' or 'by_apparent_ip'.
    def set_manager_preferred_connection(self, mode):
        return vine_set_manager_preferred_connection(self._taskvine, mode)

    ##
    # Set the minimum task_id of future submitted tasks.
    #
    # Further submitted tasks are guaranteed to have a task_id larger or equal
    # to minid.  This function is useful to make task_ids consistent in a
    # workflow that consists of sequential managers. (Note: This function is
    # rarely used).  If the minimum id provided is smaller than the last task_id
    # computed, the minimum id provided is ignored.
    #
    # @param self   Reference to the current manager object.
    # @param minid  Minimum desired task_id
    # @return Returns the actual minimum task_id for future tasks.
    def set_min_task_id(self, minid):
        return vine_set_task_id_min(self._taskvine, minid)

    ##
    # Change the project priority for the given manager.
    #
    # @param self       Reference to the current manager object.
    # @param priority   An integer that presents the priorty of this manager manager. The higher the value, the higher the priority.
    def set_priority(self, priority):
        return vine_set_priority(self._taskvine, priority)

    ##
    # Specify the number of tasks not yet submitted to the manager.
    # It is used by vine_factory to determine the number of workers to launch.
    # If not specified, it defaults to 0.
    # vine_factory considers the number of tasks as:
    # num tasks left + num tasks running + num tasks read.
    # @param self   Reference to the current manager object.
    # @param ntasks Number of tasks yet to be submitted.
    def tasks_left_count(self, ntasks):
        return vine_set_tasks_left_count(self._taskvine, ntasks)

    ##
    # Specify the catalog servers the manager should report to.
    #
    # @param self       Reference to the current manager object.
    # @param catalogs   The catalog servers given as a comma delimited list of hostnames or hostname:port
    def set_catalog_servers(self, catalogs):
        return vine_set_catalog_servers(self._taskvine, catalogs)

    ##
    # Specify a directory to write logs and staging files.
    #
    # @param self     Reference to the current manager object.
    # @param dirname  A directory name
    def set_runtime_info_path(self, dirname):
        vine_set_runtime_info_path(dirname)

    ##
    # Add a mandatory password that each worker must present.
    #
    # @param self      Reference to the current manager object.
    # @param password  The password.
    def set_password(self, password):
        return vine_set_password(self._taskvine, password)

    ##
    # Add a mandatory password file that each worker must present.
    #
    # @param self      Reference to the current manager object.
    # @param file      Name of the file containing the password.

    def set_password_file(self, file):
        return vine_set_password_file(self._taskvine, file)

    ##
    #
    # Specifies the maximum resources allowed for the default category.
    # @param self      Reference to the current manager object.
    # @param rmd       Dictionary indicating maximum values. See @ref Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A maximum of 4 cores is found on any worker:
    # >>> q.set_resources_max({'cores': 4})
    # >>> # A maximum of 8 cores, 1GB of memory, and 10GB disk are found on any worker:
    # >>> q.set_resources_max({'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def set_resources_max(self, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return vine_set_resources_max(self._taskvine, rm)

    ##
    #
    # Specifies the minimum resources allowed for the default category.
    # @param self      Reference to the current manager object.
    # @param rmd       Dictionary indicating minimum values. See @ref Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A minimum of 2 cores is found on any worker:
    # >>> q.set_resources_min({'cores': 2})
    # >>> # A minimum of 4 cores, 512MB of memory, and 1GB disk are found on any worker:
    # >>> q.set_resources_min({'cores': 4, 'memory':  512, 'disk': 1024})
    # @endcode

    def set_resources_min(self, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return vine_set_resources_min(self._taskvine, rm)

    ##
    # Specifies the maximum resources allowed for the given category.
    #
    # @param self      Reference to the current manager object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating maximum values. See @ref Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A maximum of 4 cores may be used by a task in the category:
    # >>> q.set_category_resources_max("my_category", {'cores': 4})
    # >>> # A maximum of 8 cores, 1GB of memory, and 10GB may be used by a task:
    # >>> q.set_category_resources_max("my_category", {'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def set_category_resources_max(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return vine_set_category_resources_max(self._taskvine, category, rm)

    ##
    # Specifies the minimum resources allowed for the given category.
    #
    # @param self      Reference to the current manager object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating minimum values. See @ref Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A minimum of 2 cores is found on any worker:
    # >>> q.set_category_resources_min("my_category", {'cores': 2})
    # >>> # A minimum of 4 cores, 512MB of memory, and 1GB disk are found on any worker:
    # >>> q.set_category_resources_min("my_category", {'cores': 4, 'memory':  512, 'disk': 1024})
    # @endcode

    def set_category_resources_min(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return vine_set_category_resources_min(self._taskvine, category, rm)

    ##
    # Specifies the first-allocation guess for the given category
    #
    # @param self      Reference to the current manager object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating maximum values. See @ref Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # Tasks are first tried with 4 cores:
    # >>> q.set_category_first_allocation_guess("my_category", {'cores': 4})
    # >>> # Tasks are first tried with 8 cores, 1GB of memory, and 10GB:
    # >>> q.set_category_first_allocation_guess("my_category", {'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def set_category_first_allocation_guess(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return vine_set_category_first_allocation_guess(self._taskvine, category, rm)

    ##
    # Initialize first value of categories
    #
    # @param self     Reference to the current manager object.
    # @param rm       Dictionary indicating maximum values. See @ref Task.resources_measured for possible fields.
    # @param filename JSON file with resource summaries.

    def initialize_categories(self, filename, rm):
        return vine_initialize_categories(self._taskvine, rm, filename)

    ##
    # Cancel task identified by its task_id and remove from the given manager.
    #
    # @param self   Reference to the current manager object.
    # @param id     The task_id returned from @ref submit.
    def cancel_by_task_id(self, id):
        task = None
        task_pointer = vine_cancel_by_task_id(self._taskvine, id)
        if task_pointer:
            task = self._task_table.pop(int(id))
        return task

    ##
    # Cancel task identified by its tag and remove from the given manager.
    #
    # @param self   Reference to the current manager object.
    # @param tag    The tag assigned to task using @ref Task.set_tag.
    def cancel_by_task_tag(self, tag):
        task = None
        task_pointer = vine_cancel_by_task_tag(self._taskvine, tag)
        if task_pointer:
            task = self._task_table.pop(int(id))
        return task

    ##
    # Cancel all tasks of the given category and remove them from the manager.
    #
    # @param self   Reference to the current manager object.
    # @param category The name of the category to cancel.
    def cancel_by_category(self, category):
        canceled_tasks = []
        ids_to_cancel = []

        for task in self._task_table.values():
            if task.category == category:
                ids_to_cancel.append(task.id)

        canceled_tasks = [self.cancel_by_task_id(id) for id in ids_to_cancel]
        return canceled_tasks

    ##
    # Shutdown workers connected to manager.
    #
    # Gives a best effort and then returns the number of workers given the shutdown order.
    #
    # @param self   Reference to the current manager object.
    # @param n      The number to shutdown.  0 shutdowns all workers
    def workers_shutdown(self, n=0):
        return vine_workers_shutdown(self._taskvine, n)

    ##
    # Block workers running on host from working for the manager.
    #
    # @param self   Reference to the current manager object.
    # @param host   The hostname the host running the workers.
    def block_host(self, host):
        return vine_block_host(self._taskvine, host)

    ##
    # Replaced by @ref block_host
    def blacklist(self, host):
        return self.block_host(host)

    ##
    # Block workers running on host for the duration of the given timeout.
    #
    # @param self    Reference to the current manager object.
    # @param host    The hostname the host running the workers.
    # @param timeout How long this block entry lasts (in seconds). If less than 1, block indefinitely.
    def block_host_with_timeout(self, host, timeout):
        return vine_block_host_with_timeout(self._taskvine, host, timeout)

    ##
    # See @ref block_host_with_timeout
    def blacklist_with_timeout(self, host, timeout):
        return self.block_host_with_timeout(host, timeout)

    ##
    # Unblock given host, of all hosts if host not given
    #
    # @param self   Reference to the current manager object.
    # @param host   The of the hostname the host.
    def unblock_host(self, host=None):
        if host is None:
            return vine_unblock_all(self._taskvine)
        return vine_unblock_host(self._taskvine, host)

    ##
    # See @ref unblock_host
    def blacklist_clear(self, host=None):
        return self.unblock_host(host)


    ##
    # Change keepalive interval for a given manager.
    #
    # @param self     Reference to the current manager object.
    # @param interval Minimum number of seconds to wait before sending new keepalive
    #                 checks to workers.
    def set_keepalive_interval(self, interval):
        return vine_set_keepalive_interval(self._taskvine, interval)

    ##
    # Change keepalive timeout for a given manager.
    #
    # @param self     Reference to the current manager object.
    # @param timeout  Minimum number of seconds to wait for a keepalive response
    #                 from worker before marking it as dead.
    def set_keepalive_timeout(self, timeout):
        return vine_set_keepalive_timeout(self._taskvine, timeout)

    ##
    # Tune advanced parameters.
    #
    # @param self  Reference to the current manager object.
    # @param name  The name fo the parameter to tune. Can be one of following:
    # - "resource-submit-multiplier" Treat each worker as having ({cores,memory,gpus} * multiplier) when submitting tasks. This allows for tasks to wait at a worker rather than the manager. (default = 1.0)
    # - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=10)
    # - "foreman-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)
    # - "transfer-outlier-factor" Transfer that are this many times slower than the average will be terminated.  (default=10x)
    # - "default-transfer-rate" The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)
    # - "disconnect-slow-workers-factor" Set the multiplier of the average task time at which point to disconnect a worker; disabled if less than 1. (default=0)
    # - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
    # - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
    # - "short-timeout" Set the minimum timeout when sending a brief message to a single worker. (default=5s)
    # - "long-timeout" Set the minimum timeout when sending a brief message to a foreman. (default=1h)
    # - "category-steady-n-tasks" Set the number of tasks considered when computing category buckets.
    # - "hungry-minimum" Mimimum number of tasks to consider manager not hungry. (default=10)
    # - monitor-interval Maximum number of seconds between resource monitor measurements. If less than 1, use default (5s).
    # - "wait-for-workers" Mimimum number of workers to connect before starting dispatching tasks. (default=0)
    # - "wait_retrieve_many" Parameter to alter how vine_wait works. If set to 0, vine_wait breaks out of the while loop whenever a task changes to VINE_TASK_DONE (wait_retrieve_one mode). If set to 1, vine_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
    # @param value The value to set the parameter to.
    # @return 0 on succes, -1 on failure.
    #
    def tune(self, name, value):
        return vine_tune(self._taskvine, name, value)

    ##
    # Submit a task to the manager.
    #
    # It is safe to re-submit a task returned by @ref wait.
    #
    # @param self   Reference to the current manager object.
    # @param task   A task description created from @ref taskvine::Task.
    def submit(self, task):
        task.submit_finalize(self)
        task_id = vine_submit(self._taskvine, task._task)
        self._task_table[task_id] = task
        return task_id

    ##
    # Submit a library to install on all connected workers
    #
    #
    # @param self   Reference to the current manager object.
    # @param task   A Library Task description created from create_library_from_functions or create_library_from_files
    def install_library(self, task):
        if not isinstance(task, LibraryTask):
            raise TypeError("Please provide a LibraryTask as the task argument")
        self._library_table[task.library_name] = task
        vine_manager_install_library(self._taskvine, task._task, task.library_name)

    ##
    # Remove a library from all connected workers
    #
    #
    # @param self   Reference to the current manager object.
    # @param name   Name of the library to be removed.
    def remove_library(self, name):
        del self._library_table[name]
        vine_manager_remove_library(self._taskvine, name)

    ##
    # Turn a list of python functions into a Library
    #
    # @param self            Reference to the current manager object.
    # @param name            Name of the Library to be created
    # @param function_list   List of all functions to be included in the library
    # @returns               A task to be used with @ref Manager.install_library.
    def create_library_from_functions(self, name, *function_list):
        # ensure poncho python library is available
        if poncho_available == False:
            raise ModuleNotFoundError("The poncho module is not available. Cannot create Library.")
        # positional arguments are the list of functions to include in the library
        # create a unique hash of a combination of function names and bodies
        functions_hash = package_serverize.generate_functions_hash(function_list)

        # create path for caching library code and environment based on function hash
        library_cache_path = f"{self.cache_directory}/vine-library-cache/{functions_hash}"
        library_code_path = f"{library_cache_path}/library_code.py"
        library_env_path = f"{library_cache_path}/library_env.tar.gz"

        # library cache folder doesn't exist, create it
        Path( library_cache_path ).mkdir(mode=0o755, parents=True, exist_ok=True)
        # if the library code and environment exist, move on to creating the Library Task
        if os.path.isfile(library_code_path) and os.path.isfile(library_env_path):
            pass
        else:
            print("No cached Library code and environment found, regenerating...")
            # create library code and environment
            package_serverize.serverize_library_from_code(library_cache_path, function_list, name)
            # enable correct permissions for library code
            os.chmod(library_code_path, 0o775)

        # create Task to execute the Library
        t = LibraryTask("python ./library_code.py", name)
        # declare the environment
        f = self.declare_poncho(library_env_path, cache=True)
        t.add_environment(f)
        # declare the library code as an input
        f = self.declare_file(library_code_path, cache=True)
        t.add_input(f, "library_code.py")
        return t

    ##
    # Turn Library code created with poncho_package_serverize into a Library Task
    #
    # @param self            Reference to the current manager object.
    # @param library_path    Filename of the library (i.e., the output of poncho_package_serverize)
    # @param env             Environment to run the library. Either a vine file
    #                        that expands to an environment (see @ref Task.add_environment), or a path
    #                        to a poncho environment.
    # @returns               A task to be used with @ref Manager.install_library.
    def create_library_from_serverized_files(self, name, library_path, env=None):
        if poncho_available == False:
            raise ModuleNotFoundError("The poncho module is not available. Cannot create library.")
        t = LibraryTask("python ./library_code.py", name)
        if env:
            if isinstance(env, str):
                env = self.declare_poncho(env, cache=True)
                t.add_environment(env)
            else:
                t.add_environment(env)
        f = self.declare_file(library_path, cache=True)
        t.add_input(f, "library_code.py")

        return t

    ##
    # Create a Library task from arbitrary inputs
    #
    # @param self            Reference to the current manager object
    # @param executable_path Filename of the library executable
    # @param name            Name of the library to be created
    # @param env             Environment to run the library. Either a vine file
    #                        that expands to an environment (see @ref Task.add_environment), or a path
    #                        to a poncho environment.
    # @returns               A task to be used with @ref Manager.install_library
    def create_library_from_command(self, executable_path, name, env=None):
        t = LibraryTask("./library_exe", name)
        f = self.declare_file(executable_path, cache=True)
        t.add_input(f, "library_exe")
        if env:
            if isinstance(env, str):
                env = self.declare_poncho(env, cache=True)
                t.add_environment(env)
            else:
                t.add_environment(env)
        return t

    ##
    # Wait for tasks to complete.
    #
    # This call will block until the timeout has elapsed
    #
    # @param self       Reference to the current manager object.
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.  Use an integer to set the timeout or the constant @ref
    #                   VINE_WAIT_FOREVER to block until a task has completed.
    def wait(self, timeout=VINE_WAIT_FOREVER):
        return self.wait_for_tag(None, timeout)

    ##
    # Similar to @ref wait, but guarantees that the returned task has the
    # specified tag.
    #
    # This call will block until the timeout has elapsed.
    #
    # @param self       Reference to the current manager object.
    # @param tag        Desired tag. If None, then it is equivalent to self.wait(timeout)
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.
    def wait_for_tag(self, tag, timeout=VINE_WAIT_FOREVER):
        self._update_status_display()
        task_pointer = vine_wait_for_tag(self._taskvine, tag, timeout)
        if task_pointer:
            if self.empty():
                # if last task in queue, update display
                self._update_status_display(force=True)
            task = self._task_table[vine_task_get_id(task_pointer)]
            del self._task_table[vine_task_get_id(task_pointer)]
            return task
        return None

    ##
    # Similar to @ref wait, but guarantees that the returned task has the
    # specified task_id.
    #
    # This call will block until the timeout has elapsed.
    #
    # @param self       Reference to the current manager object.
    # @param task_id        Desired task_id. If -1, then it is equivalent to self.wait(timeout)
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.
    def wait_for_task_id(self, task_id, timeout=VINE_WAIT_FOREVER):
        task_pointer = vine_wait_for_task_id(self._taskvine, task_id, timeout)
        if task_pointer:
            task = self._task_table[vine_task_get_id(task_pointer)]
            del self._task_table[vine_task_get_id(task_pointer)]
            return task
        return None

    ##
    # Should return a dictionary with information for the status display.
    # This method is meant to be overriden by custom applications.
    #
    # The dictionary should be of the form:
    #
    # { "application_info" : {"values" : dict, "units" : dict} }
    #
    # where "units" is an optional dictionary that indicates the units of the
    # corresponding key in "values".
    #
    # @param self       Reference to the current work queue object.
    #
    # For example:
    # @code
    # >>> myapp.application_info()
    # {'application_info': {'values': {'size_max_output': 0.361962, 'current_chunksize': 65536}, 'units': {'size_max_output': 'MB'}}}
    # @endcode
    def application_info(self):
        return None

    ##
    # Maps a function to elements in a sequence using taskvine
    #
    # Similar to regular map function in python
    #
    # @param self       Reference to the current manager object.
    # @param fn         The function that will be called on each element
    # @param seq        The sequence that will call the function
    # @param chunksize  The number of elements to process at once

    def map(self, fn, seq, chunksize=1):
        size = math.ceil(len(seq) / chunksize)
        results = [None] * size
        tasks = {}

        for i in range(size):
            start = i * chunksize
            end = start + chunksize

            if end > len(seq):
                p_task = PythonTask(map, fn, seq[start:])
            else:
                p_task = PythonTask(map, fn, seq[start:end])

            p_task.set_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i

        n = 0
        for i in range(size + 1):
            while not self.empty() and n < size:
                for key, value in tasks.items():
                    if value == i:
                        t_id = key
                        break
                t = self.wait_for_task_id(t_id, 1)
                if t:
                    results[tasks[t.id]] = list(t.output)
                    n += 1
                    break

        return [item for elem in results for item in elem]

    ##
    # Returns the values for a function of each pair from 2 sequences
    #
    # The pairs that are passed into the function are generated by itertools
    #
    # @param self     Reference to the current manager object.
    # @param fn       The function that will be called on each element
    # @param seq1     The first seq that will be used to generate pairs
    # @param seq2     The second seq that will be used to generate pairs
    # @param chunksize  Number of pairs to process at once (default is 1)
    # @param env      Filename of a python environment tarball (conda or poncho)
    def pair(self, fn, seq1, seq2, chunksize=1, env=None):
        def fpairs(fn, s):
            results = []

            for p in s:
                results.append(fn(p))

            return results

        size = math.ceil((len(seq1) * len(seq2)) / chunksize)
        results = [None] * size
        tasks = {}
        task = []
        num = 0
        num_task = 0

        for item in itertools.product(seq1, seq2):
            if num == chunksize:
                p_task = PythonTask(fpairs, fn, task)
                if env:
                    p_task.add_environment(env)

                p_task.set_tag(str(num_task))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num = 0
                num_task += 1
                task.clear()

            task.append(item)
            num += 1

        if len(task) > 0:
            p_task = PythonTask(fpairs, fn, task)
            p_task.set_tag(str(num_task))
            self.submit(p_task)
            tasks[p_task.id] = num_task
            num_task += 1

        n = 0
        for i in range(num_task):
            while not self.empty() and n < num_task:
                for key, value in tasks.items():
                    if value == i:
                        t_id = key
                        break
                t = self.wait_for_task_id(t_id, 10)

                if t:
                    results[tasks[t.id]] = t.output
                    n += 1
                    break

        return [item for elem in results for item in elem]

    ##
    # Reduces a sequence until only one value is left, and then returns that value.
    # The sequence is reduced by passing a pair of elements into a function and
    # then stores the result. It then makes a sequence from the results, and
    # reduces again until one value is left.
    #
    # If the sequence has an odd length, the last element gets reduced at the
    # end.
    #
    # @param self       Reference to the current manager object.
    # @param fn         The function that will be called on each element
    # @param seq        The seq that will be reduced
    # @param chunksize The number of elements per Task (for tree reduc, must be greater than 1)
    def tree_reduce(self, fn, seq, chunksize=2):
        tasks = {}
        num_task = 0

        while len(seq) > 1:
            size = math.ceil(len(seq) / chunksize)
            results = [None] * size

            for i in range(size):
                start = i * chunksize
                end = start + chunksize

                if end > len(seq):
                    p_task = PythonTask(fn, seq[start:])
                else:
                    p_task = PythonTask(fn, seq[start:end])

                p_task.set_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num_task += 1

            n = 0
            for i in range(size + 1):
                while not self.empty() and n < size:
                    for key, value in tasks.items():
                        if value == num_task - size + i:
                            t_id = key
                            break
                    t = self.wait_for_task_id(t_id, 10)

                    if t:
                        results[i] = t.output
                        n += 1
                        break

            seq = results

        return seq[0]

    ##
    # Maps a function to elements in a sequence using taskvine remote task
    #
    # Similar to regular map function in python, but creates a task to execute each function on a worker running a coprocess
    #
    # @param self       Reference to the current manager object.
    # @param fn         The function that will be called on each element. This function exists in coprocess.
    # @param seq        The sequence that will call the function
    # @param coprocess  The name of the coprocess that contains the function fn.
    # @param name       This defines the key in the event json that wraps the data sent to the coprocess.
    # @param chunksize The number of elements to process at once
    def remote_map(self, fn, seq, coprocess, name, chunksize=1):
        size = math.ceil(len(seq) / chunksize)
        results = [None] * size
        tasks = {}

        for i in range(size):
            start = i * chunksize
            end = min(len(seq), start + chunksize)

            event = json.dumps({name: seq[start:end]})
            p_task = FunctionCall(fn, event, coprocess)

            p_task.set_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i

        n = 0
        for i in range(size + 1):
            while not self.empty() and n < size:
                for key, value in tasks.items():
                    if value == i:
                        t_id = key
                        break
                t = self.wait_for_task_id(t_id, 1)
                if t:
                    results[tasks[t.id]] = list(json.loads(t.output)["Result"])
                    n += 1
                    break

        return [item for elem in results for item in elem]

    ##
    # Returns the values for a function of each pair from 2 sequences using remote task
    #
    # The pairs that are passed into the function are generated by itertools
    #
    # @param self     Reference to the current manager object.
    # @param fn       The function that will be called on each element. This function exists in coprocess.
    # @param seq1     The first seq that will be used to generate pairs
    # @param seq2     The second seq that will be used to generate pairs
    # @param coprocess  The name of the coprocess that contains the function fn.
    # @param name       This defines the key in the event json that wraps the data sent to the coprocess.
    # @param chunksize The number of elements to process at once
    def remote_pair(self, fn, seq1, seq2, coprocess, name, chunksize=1):
        size = math.ceil((len(seq1) * len(seq2)) / chunksize)
        results = [None] * size
        tasks = {}
        task = []
        num = 0
        num_task = 0

        for item in itertools.product(seq1, seq2):
            if num == chunksize:
                event = json.dumps({name: task})
                p_task = FunctionCall(fn, event, coprocess)
                p_task.set_tag(str(num_task))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num = 0
                num_task += 1
                task.clear()

            task.append(item)
            num += 1

        if len(task) > 0:
            event = json.dumps({name: task})
            p_task = FunctionCall(fn, event, coprocess)
            p_task.set_tag(str(num_task))
            self.submit(p_task)
            tasks[p_task.id] = num_task
            num_task += 1

        n = 0
        for i in range(num_task):
            while not self.empty() and n < num_task:
                for key, value in tasks.items():
                    if value == i:
                        t_id = key
                        break
                t = self.wait_for_task_id(t_id, 1)
                if t:
                    results[tasks[t.id]] = json.loads(t.output)["Result"]
                    n += 1
                    break

        return [item for elem in results for item in elem]

    ##
    # Reduces a sequence until only one value is left, and then returns that value.
    # The sequence is reduced by passing a pair of elements into a function and
    # then stores the result. It then makes a sequence from the results, and
    # reduces again until one value is left. Executes on coprocess
    #
    # If the sequence has an odd length, the last element gets reduced at the
    # end.
    #
    # @param self       Reference to the current manager object.
    # @param fn         The function that will be called on each element. Exists on the coprocess
    # @param seq        The seq that will be reduced
    # @param coprocess  The name of the coprocess that contains the function fn.
    # @param name       This defines the key in the event json that wraps the data sent to the coprocess.
    # @param chunksize The number of elements per Task (for tree reduc, must be greater than 1)
    def remote_tree_reduce(self, fn, seq, coprocess, name, chunksize=2):
        tasks = {}
        num_task = 0

        while len(seq) > 1:
            size = math.ceil(len(seq) / chunksize)
            results = [None] * size

            for i in range(size):
                start = i * chunksize
                end = min(len(seq), start + chunksize)

                event = json.dumps({name: seq[start:end]})
                p_task = FunctionCall(fn, event, coprocess)

                p_task.set_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num_task += 1

            n = 0
            for i in range(size + 1):
                while not self.empty() and n < size:
                    for key, value in tasks.items():
                        if value == num_task - size + i:
                            t_id = key
                            break
                    t = self.wait_for_task_id(t_id, 10)

                    if t:
                        results[i] = json.loads(t.output)["Result"]
                        n += 1
                        break

            seq = results

        return seq[0]


    ##
    # Declare a file obtained from the local filesystem.
    #
    # @param self    The manager to register this file
    # @param path    The path to the local file
    # @param cache   If True or 'workflow', cache the file at workers for reuse
    #                until the end of the workflow. If 'always', the file is cache until the
    #                end-of-life of the worker. Default is False (file is not cache).
    # @param peer_transfer   Whether the file can be transfered between workers when
    #                peer transfers are enabled (see @ref enable_peer_transfers). Default is True.
    # @return
    # A file object to use in @ref Task.add_input or @ref Task.add_output
    def declare_file(self, path, cache=False, peer_transfer=True):
        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_file(self._taskvine, path, flags)
        return File(f)

    ##
    # Remove file from workers, undeclare it at the manager.
    # Note that this does not remove the file's local copy at the manager, if any.
    #
    # @param self    The manager to register this file
    # @param file    The file object
    def remove_file(self, file):
        vine_remove_file(self._taskvine, file._file)

    ##
    # Declare an anonymous file has no initial content, but is created as the
    # output of a task, and may be consumed by other tasks.
    #
    # @param manager    The manager to register this file
    # @return A file object to use in @ref Task.add_input or @ref Task.add_output
    def declare_temp(self):
        f = vine_declare_temp(self._taskvine)
        return File(f)

    ##
    # Declare a file obtained from a remote URL.
    #
    # @param self    The manager to register this file
    # @param url     The url of the file.
    # @param cache   If True or 'workflow', cache the file at workers for reuse
    #                until the end of the workflow. If 'always', the file is cache until the
    #                end-of-life of the worker. Default is False (file is not cache).
    # @param peer_transfer   Whether the file can be transfered between workers when
    #                peer transfers are enabled (see @ref enable_peer_transfers). Default is True.
    # @return A file object to use in @ref Task.add_input
    def declare_url(self, url, cache=False, peer_transfer=True):
        flags = Task._determine_file_flags(cache, peer_transfer)

        if not isinstance(url, str):
            raise TypeError(f"url {url} is not a str")

        f = vine_declare_url(self._taskvine, url, flags)
        return File(f)

    ##
    # Declare a file created from a buffer in memory.
    #
    # @param self    The manager to register this file
    # @param buffer  The contents of the buffer, or None for an empty output buffer
    # @param cache   If True or 'workflow', cache the file at workers for reuse
    #                until the end of the workflow. If 'always', the file is cache until the
    #                end-of-life of the worker. Default is False (file is not cache).
    # @param peer_transfer   Whether the file can be transfered between workers when
    #                peer transfers are enabled (see @ref enable_peer_transfers). Default is True.
    # @return A file object to use in @ref Task.add_input
    #
    # For example:
    # @code
    # >>> s = "hello pirate ♆"
    # >>> f = m.declare_buffer(bytes(s, "utf-8"))
    # >>> print(f.contents())
    # >>> "hello pirate ♆"
    # @endcode
    def declare_buffer(self, buffer=None, cache=False, peer_transfer=True):
        # because of the swig typemap, vine_declare_buffer(m, buffer, size) is changed
        # to a function with just two arguments.
        flags = Task._determine_file_flags(cache, peer_transfer)
        if isinstance(buffer, str):
            buffer = bytes(buffer, "utf-8")
        f = vine_declare_buffer(self._taskvine, buffer, flags)
        return File(f)

    ##
    # Declare a file created by executing a mini-task.
    #
    # @param self     The manager to register this file
    # @param minitask The task to execute in order to produce a file
    # @return A file object to use in @ref Task.add_input
    def declare_minitask(self, minitask, cache=False, peer_transfer=True):
        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_mini_task(self._taskvine, minitask._task, flags)

        # minitasks are freed when the manager frees its related file structure
        minitask._manager_will_free = True

        return File(f)

    ##
    # Declare a file created by by unpacking a tar file.
    #
    # @param manager    The manager to register this file
    # @param tarball    The file object to un-tar
    # @return A file object to use in @ref Task.add_input
    def declare_untar(self, tarball, cache=False, peer_transfer=True):
        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_untar(self._taskvine, tarball._file, flags)
        return File(f)

    ##
    # Declare a file that sets up a poncho environment
    #
    # @param self    The manager to register this file
    # @param package The poncho environment tarball. Either a vine file or a
    #                string representing a local file.
    # @return A file object to use in @ref Task.add_input
    def declare_poncho(self, package, cache=False, peer_transfer=True):
        if isinstance(package, str):
            package = self.declare_file(package, cache=True)

        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_poncho(self._taskvine, package._file, flags)
        return File(f)

    ##
    # Declare a file create a file by unpacking a starch package.
    #
    # @param self    The manager to register this file
    # @param starch  The startch .sfx file. Either a vine file or a string
    #                representing a local file.
    # @return A file object to use in @ref Task.add_input
    def declare_starch(self, starch, cache=False, peer_transfer=True):
        if isinstance(starch, str):
            starch = self.declare_file(starch, cache=True)

        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_starch(self._taskvine, starch._file, flags)
        return File(f)

    ##
    # Declare a file from accessible from an xrootd server.
    #
    # @param self   The manager to register this file.
    # @param source The URL address of the root file in text form as: "root://XROOTSERVER[:port]//path/to/file"
    # @param proxy  A @ref File of the X509 proxy to use. If None, the
    #               environment variable X509_USER_PROXY and the file
    #               "$TMPDIR/$UID" are considered in that order. If no proxy is
    #               present, the transfer is tried without authentication.
    # @param env    If not None, an environment file (e.g poncho or starch, see Task.add_environment)
    #               that contains the xrootd executables. Otherwise assume xrootd is available
    #               at the worker.
    # @param cache  If True or 'workflow', cache the file at workers for reuse
    #               until the end of the workflow. If 'always', the file is cache until the
    #               end-of-life of the worker. Default is False (file is not cache).
    # @param peer_transfer   Whether the file can be transfered between workers when
    #                peer transfers are enabled (see @ref enable_peer_transfers). Default is True.
    # @return A file object to use in @ref Task.add_input
    def declare_xrootd(self, source, proxy=None, env=None, cache=False, peer_transfer=True):
        proxy_c = None
        if proxy:
            proxy_c = proxy._file

        env_c = None
        if env:
            env_c = env._file

        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_xrootd(self._taskvine, source, proxy_c, env_c, flags)
        return File(f)

    ##
    # Declare a file from accessible from an xrootd server.
    #
    # @param self   The manager to register this file.
    # @param server The chirp server address of the form "hostname[:port"]"
    # @param source The name of the file in the server
    # @param ticket If not None, a file object that provides a chirp an authentication ticket
    # @param env    If not None, an environment file (e.g poncho or starch)
    #               that contains the chirp executables. Otherwise assume chirp is available
    #               at the worker.
    # @param cache   If True or 'workflow', cache the file at workers for reuse
    #                until the end of the workflow. If 'always', the file is cache until the
    #                end-of-life of the worker. Default is False (file is not cache).
    # @param peer_transfer   Whether the file can be transfered between workers when
    #                peer transfers are enabled (see @ref enable_peer_transfers). Default is True.
    # @return A file object to use in @ref Task.add_input
    def declare_chirp(self, server, source, ticket=None, env=None, cache=False, peer_transfer=True):
        ticket_c = None
        if ticket:
            ticket_c = ticket._file

        env_c = None
        if env:
            env_c = env._file

        flags = Task._determine_file_flags(cache, peer_transfer)
        f = vine_declare_chirp(self._taskvine, server, source, ticket_c, env_c, flags)
        return File(f)




##
# \class FunctionCall
#
# TaskVine FunctionCall object
#
# This class represents a task specialized to execute functions in a Library running on a worker.
class FunctionCall(Task):
    ##
    # Create a new FunctionCall specification.
    #
    # @param self       Reference to the current FunctionCall object.
    # @param fn         The name of the function to be executed on the coprocess
    # @param coprocess  The name of the coprocess which has the function you wish to execute. The coprocess should have a name() method that returns this
    # @param command    The shell command line to be exected by the task.
    # @param args       positional arguments used in function to be executed by task. Can be mixed with kwargs
    # @param kwargs	    keyword arguments used in function to be executed by task.
    def __init__(self, fn, coprocess, *args, **kwargs):
        Task.__init__(self, fn)
        self._event = {}
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args
        Task.set_coprocess(self, "library_coprocess:" + coprocess)

    ##
    # Finalizes the task definition once the manager that will execute is run.
    # This function is run by the manager before registering the task for
    # execution.
    #
    # @param self 	Reference to the current python task object
    # @param manager Manager to which the task was submitted
    def submit_finalize(self, manager):
        f = manager.declare_buffer(json.dumps(self._event))
        self.add_input(f, "infile")

    ##
    # Specify function arguments. Accepts arrays and dictionaries. This
    # overrides any arguments passed during task creation
    # @param self             Reference to the current remote task object
    # @param args             An array of positional args to be passed to the function
    # @param kwargs           A dictionary of keyword arguments to be passed to the function
    def set_fn_args(self, args=[], kwargs={}):
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args

    ##
    # Specify how the remote task should execute
    # @param self                     Reference to the current remote task object
    # @param remote_task_exec_method  Can be either of "fork" or "direct".
    # Fork creates a child process to execute the function and direct
    # has the worker directly call the function.
    def set_exec_method(self, remote_task_exec_method):
        if remote_task_exec_method not in ["fork", "direct"]:
            print("Error, vine_exec_method must either be fork or direct, choosing fork by default")
            remote_task_exec_method = "fork"
        self._event["remote_task_exec_method"] = remote_task_exec_method


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
#     # normal WQ setup stuff
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


def rmsummary_snapshots(self):
    if self.snapshots_count < 1:
        return None

    snapshots = []
    for i in range(0, self.snapshots_count):
        snapshot = rmsummary_get_snapshot(self, i)
        snapshots.append(snapshot)
    return snapshots


rmsummary.snapshots = property(rmsummary_snapshots)

##
# \class LibraryTask
#
# TaskVine LibraryTask object
#
# This class represents a task specialized to running a coprocess that contains Python functions at the worker
class LibraryTask(Task):
    ##
    # Create a new LibraryTask task specification.
    #
    # @param self       Reference to the current remote task object.
    # @param fn         The command for this LibraryTask to run
    # @param name       The name of this Library.
    def __init__(self, fn, name):
        Task.__init__(self, fn)
        self.library_name = "library_coprocess:" + name
