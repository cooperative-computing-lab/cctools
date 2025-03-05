##
# @package ndcctools.taskvine.task
#
# This module provides the classes to construct tasks to submit for execution to a
# TaskVine manager.
#

# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
from . import cvine
from .file import File

import copy
import os
import sys
import textwrap
import uuid
import cloudpickle


##
# @class ndcctools.taskvine.task.Task
#
# TaskVine Task object
#
# This class is used to create a task specification to be submitted to a @ref ndcctools.taskvine.manager.Manager.
class Task(object):
    ##
    # Create a new task specification.
    #
    # @param self       Reference to the current task object.
    # @param command    The shell command line to be exected by the task.
    # @param task_info  Optional dictionary containing specified task parameters.
    def __init__(self, command, **task_info):
        self._task = None

        self._manager = None  # set when task is submitted

        if isinstance(command, dict):
            raise TypeError(f"{command} is not a str. Did you mean **{command}?")

        # python dels regular tasks when they go out of scope.
        # mini tasks are freed when the manager frees their associated file structure
        self._manager_will_free = False

        self._task = cvine.vine_task_create(command)
        if not self._task:
            raise Exception("Unable to create internal Task structure")

        attributes = [
            "library_required",
            "library_provided",
            "scheduler", "tag", "category",
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
                    parameters = {"remote_name": value}
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
                    parameters = {"remote_name": value}
                else:
                    raise TypeError(f"{value} is not a str or dict")
                print(f"self.add_output({key}, {parameters})")
                self.add_output(key, **parameters)

        if "env" in task_info:
            for key, value in task_info["env"].items():
                self.set_env_var(key, value)

    def _set_from_dict(self, task_info, key):
        try:
            value = task_info[key]
            method = f"set_{key}"
            setter = getattr(self, method)
            setter(value)
        except KeyError:
            pass

    def __del__(self):
        try:
            if not self._task:
                return
            if self._manager_will_free:
                # e.g., for a minitask that won't be in self.manager._task_table
                # otherwise the task gets a double-free
                return
            if self.manager and self.id in self.manager._task_table:
                # interpreter is shutting down. Don't delete task here so that manager
                # does not get memory errors
                return
            cvine.vine_task_delete(self._task)
            self._task = None
        except TypeError:
            # modules were freed before task (e.g. interpreter shutdown)
            pass

    @staticmethod
    def _determine_mount_flags(watch=False, failure_only=False, success_only=False, strict_input=False, mount_symlink=False):
        flags = cvine.VINE_TRANSFER_ALWAYS
        if watch:
            flags |= cvine.VINE_WATCH
        if failure_only:
            flags |= cvine.VINE_FAILURE_ONLY
        if success_only:
            flags |= cvine.VINE_SUCCESS_ONLY
        if strict_input:
            flags |= cvine.VINE_FIXED_LOCATION
        if mount_symlink:
            flags |= cvine.VINE_MOUNT_SYMLINK
        return flags

    @staticmethod
    def _determine_file_flags(peer_transfer=False, unlink_when_done=False):
        flags = 0
        if not peer_transfer:
            flags |= cvine.VINE_PEER_NOSHARE
        if unlink_when_done:
            flags |= cvine.VINE_UNLINK_WHEN_DONE
        return flags

    @staticmethod
    def _determine_cache_level(cache=False):
        cache_level = cvine.VINE_CACHE_LEVEL_TASK

        if cache is True or cache == "workflow":
            cache_level = cvine.VINE_CACHE_LEVEL_WORKFLOW
        elif cache == "worker":
            cache_level = cvine.VINE_CACHE_LEVEL_WORKER
        elif cache == "forever":
            cache_level = cvine.VINE_CACHE_LEVEL_FOREVER
        elif not cache or cache == "task":
            cache_level = cvine.VINE_CACHE_LEVEL_TASK
        else:
            raise ValueError(f"{cache} is not a valid cache level")

        return cache_level

    ##
    # Finalizes the task definition once the manager that will execute is run.
    # This function is run by the manager before registering the task for
    # execution.
    #
    # @param self 	Reference to the current python task object
    def submit_finalize(self):
        pass

    @property
    def manager(self):
        return self._manager

    @manager.setter
    def manager(self, manager):
        self._manager = manager

    ##
    # Return a copy of this task
    #
    def clone(self):
        """Return a (deep)copy this task that can also be submitted to the ndcctools.taskvine."""
        new = copy.copy(self)
        new._task = cvine.vine_task_clone(self._task)
        return new

    ##
    # Set the command to be executed by the task.
    #
    # @param self       Reference to the current task object.
    # @param command    The command to be executed.
    def set_command(self, command):
        return cvine.vine_task_set_command(self._task, command)

    ##
    # Compute the name of a given library
    # @param self          Reference to the current task object.
    # @param library       The library or the name of the library
    def _compute_library_name(self, library):
        library_name = None
        if isinstance(library, Task):
            try:
                library_name = library.provides_library()
            except Exception:
                pass
        else:
            library_name = library

        if not isinstance(library, str):
            raise ValueError(f"{library} is not a valid library")

        return library_name

    ##
    # Set the name of the library at the worker that should execute the task's command.
    # This is not needed for regular tasks.
    #
    # @param self          Reference to the current task object.
    # @param library       The library or the name of the library
    def set_library_required(self, library):
        library_name = self._compute_library_name(library)
        if self.get_libray_provided():
            raise ValueError(
                f"A task cannot both provide ({library_name}) and require ({library_name}) a library."
            )
        return cvine.vine_task_set_library_required(self._task, library_name)

    ##
    # Get the name of the library at the worker that should execute the task's command.
    #
    # @param self Reference to the current task object.
    def get_library_required(self):
        return cvine.vine_task_get_library_required(self._task)

    ##
    # Deprecated, see set_library_required
    def needs_library(self, library):
        return self.set_library_required(library)

    ##
    # Set the library name provided by this task.
    # This is not needed for regular tasks.
    #
    # @param self Reference to the current task object.
    # @param library_name The name of the library.
    def set_library_provided(self, library_name):
        if self.get_library_required():
            raise ValueError(
                f"A task cannot both provide ({library_name}) and require ({self.get_library_required()}) a library."
            )
        return cvine.vine_task_set_library_provided(self._task, library_name)

    ##
    # Get the name of the library at the worker that should execute the task's command.
    #
    # @param self Reference to the current task object.
    def get_libray_provided(self):
        return cvine.vine_task_get_library_provided(self._task)

    ##
    # Deprecated, see set_library_provided
    def provides_library(self, library):
        return self.set_library_provided(library)

    ##
    # Set the number of concurrent functions a library can run.
    # This is not needed for regular tasks.
    #
    # @param self Reference to the current task object.
    # @param nslots The maximum number of concurrent functions this library can run.
    def set_function_slots(self, nslots):
        return cvine.vine_task_set_function_slots(self._task, nslots)

    ##
    # Set the execution mode of functions in a library.
    # This is not needed for regular tasks.
    #
    # @param self       Reference to the current task object.
    # @param exec_mode  The execution mode of functions in a library. Either 'fork' or 'direct'.
    def set_function_exec_mode_from_string(self, exec_mode):
        return cvine.vine_task_set_function_exec_mode_from_string(self._task, exec_mode)

    ##
    # Set the worker selection scheduler for task.
    #
    # @param self       Reference to the current task object.
    # @param scheduler  One of the following schedulers to use in assigning a
    #                   task to a worker. See @ref vine_schedule_t for
    #                   possible values.
    def set_scheduler(self, scheduler):
        return cvine.vine_task_set_scheduler(self._task, scheduler)

    ##
    # Attach a user defined logical name to the task.
    #
    # @param self       Reference to the current task object.
    # @param tag        The tag to attach to task.
    def set_tag(self, tag):
        return cvine.vine_task_set_tag(self._task, tag)

    ##
    # Label the task with the given category. It is expected that tasks with the
    # same category have similar resources requirements (e.g. to disconnect slow workers).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the category
    def set_category(self, name):
        return cvine.vine_task_set_category(self._task, name)

    ##
    # Label the task with the given user-defined feature. Tasks with the
    # feature will only run on workers that provide it (see worker's
    # --feature option).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the feature.
    def add_feature(self, name):
        return cvine.vine_task_add_feature(self._task, name)

    ##
    # Add any input object to a task.
    #
    # @param self          Reference to the current task object.
    # @param file          A file object of class @ref ndcctools.taskvine.file.File, such as from @ref ndcctools.taskvine.manager.Manager.declare_file, @ref ndcctools.taskvine.manager.Manager.declare_buffer, @ref ndcctools.taskvine.manager.Manager.declare_url, etc.
    # @param remote_name   The name of the file at the execution site.
    # @param strict_input  Whether the file should be transfered to the worker
    #                      for execution. If no worker has all the input files already cached marked
    #                      as strict inputs for the task, the task fails.
    #
    # For example:
    # @code
    # >>> url = m.declare_url(http://somewhere.edu/data.tgz)
    # >>> f = m.declare_untar(url)
    # >>> task.add_input(f,"data")
    # @endcode
    def add_input(self, file, remote_name, strict_input=False, mount_symlink=False):
        # SWIG expects strings
        if not isinstance(remote_name, str):
            raise TypeError(f"remote_name {remote_name} is not a str")

        flags = Task._determine_mount_flags(strict_input=strict_input, mount_symlink=mount_symlink)

        if cvine.vine_task_add_input(self._task, file._file, remote_name, flags) == 0:
            raise ValueError("invalid file description")

    ##
    # Add any output object to a task.
    #
    # @param self          Reference to the current task object.
    # @param file          A file object of class @ref ndcctools.taskvine.file.File, such as from @ref ndcctools.taskvine.manager.Manager.declare_file, or @ref ndcctools.taskvine.manager.Manager.declare_buffer @ref ndcctools.taskvine.task.Task.add_input
    # @param remote_name   The name of the file at the execution site.
    # @param watch         Watch the output file and send back changes as the task runs.
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
        if cvine.vine_task_add_output(self._task, file._file, remote_name, flags) == 0:
            raise ValueError("invalid file description")

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
        return cvine.vine_task_set_snapshot_file(self._task, filename)

    ##
    # Add a Starch package as an execution context.
    # The file given must refer to a (unpacked) package
    # containing libraries captured by the <tt>starch</tt> command.
    # The task will execute using this package as its environment.
    # @param t A task object.
    # @param f A file containing an unpacked Starch package.
    def add_starch_package(self, file):
        return cvine.vine_task_add_starch_package(self._task, file._file)

    ##
    # Add a Poncho package as an execution context.
    # The file given must refer to a (unpacked) PONCHO package,
    # containing a set of Python modules needed by the task.
    # The task will execute using this package as its Python environment.
    # @param t A task object.
    # @param f A file containing an unpacked Poncho package.
    def add_poncho_package(self, file):
        return cvine.vine_task_add_poncho_package(self._task, file._file)

    ##
    # Adds an execution context to the task.
    # The context file given must expand to a directory containing
    # (at a minimum) a file
    # named bin/run_in_env that will perform any desired setup
    # (e.g. setting PATH, LD_LIBRARY_PATH, PYTHONPATH), execute the given command,
    # and then perform any desired cleanup.  The context directory
    # may also include any support files or libraries needed by the task.
    # If specified multiple times, execution contexts are
    # nested in the order given (i.e. first added is the first applied).
    # @see add_poncho_package
    # @see add_starch_package
    # @param t A task object.
    # @param f The execution context file.
    def add_execution_context(self, f):
        return cvine.vine_task_add_execution_context(self._task, f._file)

    # Deprecated, for backwards compatibility.
    def add_environment(self, f):
        return cvine.vine_task_add_environment(self._task, f._file)

    ##
    # Indicate the number of times the task should be retried. If less than 1 (the
    # default), the task is tried indefinitely. A task that did not succeed
    # after the given number of retries is returned with result
    # "max retries".
    def set_retries(self, max_retries):
        return cvine.vine_task_set_retries(self._task, max_retries)

    ##
    # Indicate the number of times the task can be returned to the manager
    # without being executed. If less than 0 (the default), the task is tried indefinitely.
    # A task that did not succeed after the given number of retries is returned
    # with result "forsaken".
    def set_max_forsaken(self, max_forsaken):
        return cvine.vine_task_set_max_forsaken(self._task, max_forsaken)

    ##
    # Indicate the number of cores required by this task.
    def set_cores(self, cores):
        return cvine.vine_task_set_cores(self._task, cores)

    ##
    # Indicate the memory (in MB) required by this task.
    def set_memory(self, memory):
        return cvine.vine_task_set_memory(self._task, memory)

    ##
    # Indicate the disk space (in MB) required by this task.
    def set_disk(self, disk):
        return cvine.vine_task_set_disk(self._task, disk)

    ##
    # Indicate the number of GPUs required by this task.
    def set_gpus(self, gpus):
        return cvine.vine_task_set_gpus(self._task, gpus)

    ##
    # Indicate the the priority of this task (larger means better priority, default is 0).
    def set_priority(self, priority):
        return cvine.vine_task_set_priority(self._task, priority)

    # Indicate the maximum end time (absolute, in microseconds from the Epoch) of this task.
    # This is useful, for example, when the task uses certificates that expire.
    # If less than 1, or not specified, no limit is imposed.
    def set_time_end(self, useconds):
        return cvine.vine_task_set_time_end(self._task, int(useconds))

    # Indicate the minimum start time (absolute, in microseconds from the Epoch) of this task.
    # Task will only be submitted to workers after the specified time.
    # If less than 1, or not specified, no limit is imposed.
    def set_time_start(self, useconds):
        return cvine.vine_task_set_time_start(self._task, int(useconds))

    # Indicate the maximum running time (in seconds) for a task in a
    # worker (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def set_time_max(self, useconds):
        return cvine.vine_task_set_time_max(self._task, int(useconds))

    # Indicate the minimum running time (in seconds) for a task in a worker
    # (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def set_time_min(self, seconds):
        return cvine.vine_task_set_time_min(self._task, int(seconds))

    ##
    # Set this environment variable before running the task.
    # If value is None, then variable is unset.
    def set_env_var(self, name, value=None):
        return cvine.vine_task_set_env_var(self._task, name, value)

    ##
    # Set a name for the resource summary output directory from the monitor.
    def set_monitor_output(self, directory):
        return cvine.vine_task_set_monitor_output(self._task, directory)

    ##
    # Get the user-defined logical name for the task.
    #
    # @code
    # >>> print(t.tag)
    # @endcode
    @property
    def tag(self):
        return cvine.vine_task_get_tag(self._task)

    ##
    # Get the category name for the task.
    #
    # @code
    # >>> print(t.category)
    # @endcode
    @property
    def category(self):
        return cvine.vine_task_get_category(self._task)

    ##
    # Get the shell command executed by the task.
    # @code
    # >>> print(t.command)
    # @endcode
    @property
    def command(self):
        return cvine.vine_task_get_command(self._task)

    ##
    # Get the state of the task.
    # @code
    # >>> print(t.command)
    # @endcode
    @property
    def state(self):
        return cvine.vine_task_get_state(self._task)

    ##
    # Get the standard output of the task. Must be called only after the task
    # completes execution.
    # @code
    # >>> print(t.std_output)
    # @endcode
    @property
    def std_output(self):
        return cvine.vine_task_get_stdout(self._task)

    ##
    # Get the standard output of the task. (Same as t.std_output for regular
    # taskvine tasks) Must be called only after the task completes execution.
    # If this task is a FunctionCall task then we apply some transformations
    # as FunctionCall returns a specifically formatted result.
    # @code
    # >>> print(t.output)
    # @endcode
    @property
    def output(self):
        return cvine.vine_task_get_stdout(self._task)

    ##
    # Get the task id number. Must be called only after the task was submitted.
    # @code
    # >>> print(t.id)
    # @endcode
    @property
    def id(self):
        return cvine.vine_task_get_id(self._task)

    ##
    # Get the exit code of the command executed by the task. Must be called only
    # after the task completes execution.
    # @code
    # >>> print(t.exit_code)
    # @endcode
    @property
    def exit_code(self):
        return cvine.vine_task_get_exit_code(self._task)

    ##
    # Return a string that explains the result of a task.
    # Must be called only after the task completes execution.
    #
    # Possible results are:
    # "success"
    # "input missing"
    # "output missing"
    # "stdout missing"
    # "signal"
    # "resource exhaustion"
    # "max end time"
    # "unknown"
    # "forsaken"
    # "max retries"
    # "max wall time"
    # "monitor error"
    # "output transfer error"
    # "fixed location missing"
    #
    # @code
    # >>> print(t.result)
    # 'success'
    # @endcode
    @property
    def result(self):
        result = cvine.vine_result_string(cvine.vine_task_get_result(self._task))
        return result.lower().replace("_", " ")

    ##
    # Return True if task executed and its command terminated normally.
    # If True, the exit code of the command can be retrieved with @ref exit_code.
    # If False, the error condition can be retrieved with @ref result.
    # It must be called only after the task completes execution.
    # @code
    # >>> # completed tasks with a failed command execution:
    # >>> print(t.completed())
    # True
    # >>> print(t.exit_code)
    # 1
    # >>> # task with an error condition:
    # >>> print(t.completed())
    # False
    # >>> print(t.result)
    # max retries
    # @endcode
    def completed(self):
        return self.result == "success"

    ##
    # Return True if task executed successfully, (i.e. its command terminated
    # normally with exit code 0 and produced all its declared output files).
    # Differs from @ref ndcctools.taskvine.task.Task.completed in that the exit code of the command should
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
    def get_metric(self, name):
        return cvine.vine_task_get_metric(self._task, name)

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    #
    # @code
    # >>> print(t.host)
    # @endcode
    @property
    def addrport(self):
        return cvine.vine_task_get_addrport(self._task)

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    #
    # @code
    # >>> print(t.host)
    # @endcode
    @property
    def hostname(self):
        return cvine.vine_task_get_hostname(self._task)

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
        if not self._task:
            return None
        return cvine.vine_task_get_resources(self._task, "measured")

    ##
    # Get the resources the task exceeded. For valid field see @ref ndcctools.taskvine.task.Task.resources_measured.
    #
    @property
    def limits_exceeded(self):
        if not self._task:
            return None

        measured = cvine.vine_task_get_resources(self._task, "measured")
        if not measured:
            return None

        return measured.limits_exceeded

    ##
    # Get the resources the task requested to run. For valid fields see
    # @ref ndcctools.taskvine.task.Task.resources_measured.
    #
    @property
    def resources_requested(self):
        if not self._task:
            return None
        return cvine.vine_task_get_resources(self._task, "requested")

    ##
    # Get the resources allocated to the task in its latest attempt. For valid
    # fields @ref ndcctools.taskvine.task.Task.resources_measured.
    #
    @property
    def resources_allocated(self):
        if not self._task:
            return None
        return cvine.vine_task_get_resources(self._task, "allocated")

    ##
    # Adds inputs for nopen library and rules file and sets LD_PRELOAD
    #
    def add_nopen(self, manager):
        try:
            vine_dir = os.environ['CCTOOLS_HOME']
            self.add_input(manager.declare_file(f"{vine_dir}/lib-nopen.so"), "./lib-nopen.so")
        except KeyError:
            self.add_input(manager.declare_file("./lib-nopen.so"), "./lib-nopen.so")
            self.add_input(manager.declare_file("./rules.txt"), "./rules.txt")

        self.set_env_var("LD_PRELOAD", "./lib-nopen.so")


##
# @class ndcctools.taskvine.PythonTask
#
# TaskVine PythonTask object
#
# The class represents a Task specialized to execute remote Python code.
#
class PythonTask(Task):
    ##
    # Creates a new python task
    #
    # @param self 	Reference to the current python task object
    # @param func	python function to be executed by task
    # @param args	arguments used in function to be executed by task
    # @param kwargs	keyword arguments used in function to be executed by task
    def __init__(self, func, *args, **kwargs):
        self._id = str(uuid.uuid4())

        self._output_loaded = False
        self._output = None

        self._serialize_output = True

        self._out_name_file = self._id

        self._command = self._python_function_command()

        self._tmp_output_enabled = False
        self._cache_output = False

        # vine File object that will contain the output of this function
        self._output_file = None

        # vine File object with the serialized arguments to the function
        self._input_file = None

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
    def submit_finalize(self):
        super().submit_finalize()
        self._add_inputs_outputs(self.manager, *self._fn_def)
        self._fn_def = None  # avoid possible memory leak

    # remove any ancillary files generated
    # if __del__ is never called, or called too late (e.g. on interpreter shutdown),
    # then temp files will be deleted in the atexit of the manager staging directory
    def __del__(self):
        try:
            if self._input_file:
                self.manager.undeclare_file(self._input_file)
                self._input_file = None
            super().__del__()
        except TypeError:
            # in case the interpreter is shuting down. staging files will be deleted by manager atexit function.
            pass

    ##
    # Marks the output of this task to stay at the worker.
    # Functions that consume the output of this tasks have to
    # add self.output_file as an input, and cloudpickle.load() it.
    #
    # E.g.:
    #
    # @code
    # ta = PythonTask(fn, ...)
    # ta.enable_temp_output()
    # tid = m.submit(ta)
    #
    # t = m.wait(...)
    # if t.id == tid:
    #   tb = PythonTask(fn_with_tmp, "ta_output.file")
    #   tb.add_input(ta.output_file, "ta_output.file")
    #   m.submit(tb)
    # @endcode
    #
    # where fn_with_tmp may look something like this:
    #
    # @code
    # def fn_with_tmp(filename):
    #   import cloudpickle
    #   with open(filename) as f:
    #     data = cloudpickle.load(f)
    # @endcode
    #
    # @param self 	Reference to the current python task object
    def enable_temp_output(self):
        self._tmp_output_enabled = True

    def disable_temp_output(self):
        self._tmp_output_enabled = False

    ##
    # Set the cache behavior for the output of the task.
    # @param cache   If True or 'workflow', cache the file at workers for reuse
    #                until the end of the workflow. If 'always', the file is cache until the
    #                end-of-life of the worker. Default is False (file is not cache).
    def set_output_cache(self, cache=False):
        self._cache_output = cache

    ##
    # Returns the ndcctools.taskvine.file.File object that
    # represents the output of this task.
    @property
    def output_file(self):
        return self._output_file

    ##
    # returns the result of a python task as a python variable
    #
    # @param self	reference to the current python task object
    @property
    def output(self):
        if self._tmp_output_enabled:
            raise ValueError("temp output was enabled for this task, thus its output is not available locallly.")

        if not self._output_loaded:
            if self.successful():
                try:
                    with open(self._output_file.source(), "rb") as f:
                        if self._serialize_output:
                            self._output = cloudpickle.load(f)
                        else:
                            self._output = f.read()
                except Exception as e:
                    self._output = e
            else:
                self._output = PythonTaskNoResult()
                print(self.std_output)
            self._output_loaded = True
        return self._output

    ##
    # Disables serialization of results to disk when writing to a file for transmission.
    # WARNING: Only do this if the function itself encodes the output in a way amenable
    # for serialization.
    #
    # @param self 	Reference to the current python task object
    def disable_output_serialization(self):
        self._serialize_output = False

    def _python_function_command(self, remote_env_dir=None):
        if remote_env_dir:
            py_exec = "python"
        else:
            py_exec = f"python{sys.version_info[0]}"

        command = f"{py_exec} w_{self._id} f_{self._id} a_{self._id} o_{self._id}"
        return command

    def _add_inputs_outputs(self, manager, func, args, kwargs):
        self.add_input(self._fn_wrapper(manager, self._serialize_output), f"w_{self._id}")
        self.add_input(self._fn_buffer(manager, func), f"f_{self._id}")

        name = os.path.join(manager.staging_directory, "arguments", self._id)
        with open(name, "wb") as wf:
            cloudpickle.dump([args, kwargs], wf)
        self._input_file = manager.declare_file(name, unlink_when_done=True)
        self.add_input(self._input_file, f"a_{self._id}")

        if self._tmp_output_enabled:
            self._output_file = self.manager.declare_temp()
        else:
            name = os.path.join(manager.staging_directory, "outputs", self._id)
            self._output_file = manager.declare_file(name, cache=self._cache_output, unlink_when_done=False)
        self.add_output(self._output_file, f"o_{self._id}")

    def _fn_wrapper(self, manager, serialize):
        base = f"py_wrapper_{int(bool(serialize))}"
        if base not in manager._function_buffers:
            name = os.path.join(manager.staging_directory, base)
            with open(name, "w") as f:
                f.write(textwrap.dedent(f"""
                                        import sys
                                        import cloudpickle
                                        fn, args, out = sys.argv[1:]

                                        with open(fn, "rb") as f:
                                            exec_function = cloudpickle.load(f)
                                        with open(args, "rb") as f:
                                            args, kwargs = cloudpickle.load(f)

                                        error = 0
                                        try:
                                            exec_out = exec_function(*args, **kwargs)
                                        except Exception as e:
                                            exec_out = e
                                            error = e
                                        else:
                                            with open(out, "wb") as f:
                                                if {serialize}:
                                                    cloudpickle.dump(exec_out, f)
                                                else:
                                                    f.write(exec_out)
                                        finally:
                                            if error:
                                                import traceback
                                                traceback.print_exc()
                                                raise error
                                        """))
                manager._function_buffers[base] = manager.declare_file(name, cache=True)
        return manager._function_buffers[base]

    def _fn_buffer(self, manager, fn):
        if fn not in manager._function_buffers:
            load = cloudpickle.dumps(fn)
            manager._function_buffers[fn] = manager.declare_buffer(load, cache=True)
        return manager._function_buffers[fn]


class PythonTaskNoResult(Exception):
    pass


##
# @class ndcctools.taskvine.task.FunctionCall
#
# TaskVine FunctionCall object
#
# This class represents a task specialized to execute functions in a Library running on a worker.
class FunctionCall(PythonTask):
    ##
    # Create a new FunctionCall specification.
    #
    # @param self       Reference to the current FunctionCall object.
    # @param library    The library, or name of the library which has the function you wish to execute.
    # @param fn         The name of the function to be executed on the library.
    # @param args       positional arguments used in function to be executed by task. Can be mixed with kwargs
    # @param kwargs	keyword arguments used in function to be executed by task.
    def __init__(self, library, fn, *args, **kwargs):
        super().__init__(self, None)

        # function calls at worker only need the name of the function.
        self.set_command(fn)

        self._event = {}
        self._event["fn_args"] = args
        self._event["fn_kwargs"] = kwargs

        self._saved_output = None
        self.set_library_required(library)

    ##
    # Finalizes the task definition once the manager that will execute is run.
    # This function is run by the manager before registering the task for
    # execution.
    #
    # @param self 	Reference to the current python task object
    def submit_finalize(self):
        library_name = self.get_library_required()
        if not self.manager.check_library_exists(library_name):
            raise ValueError(f"invalid library name \'{library_name}\'")

        name = os.path.join(self.manager.staging_directory, "arguments", self._id)
        with open(name, "wb") as wf:
            cloudpickle.dump(self._event, wf)
        self._input_file = self.manager.declare_file(name, unlink_when_done=True, cache=False, peer_transfer=True)

        if self._tmp_output_enabled:
            self._output_file = self.manager.declare_temp()
        else:
            name = os.path.join(self.manager.staging_directory, "outputs", self._id)
            self._output_file = self.manager.declare_file(name, cache=self._cache_output, unlink_when_done=False)

        self._event = None  # free args memory. Once in a file they are not needed anymore.
        self.add_input(self._input_file, "infile")
        self.add_output(self._output_file, "outfile")

    ##
    # Specify function arguments. Accepts arrays and dictionaries. This
    # overrides any arguments passed during task creation
    # @param self             Reference to the current remote task object
    # @param args             An array of positional args to be passed to the function
    # @param kwargs           A dictionary of keyword arguments to be passed to the function
    def set_fn_args(self, args=[], kwargs={}):
        self._event["fn_args"] = args
        self._event["fn_kwargs"] = kwargs

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
    # Retrieve output, handles cleanup, and returns result or failure reason.
    @property
    def output(self):
        if self._tmp_output_enabled:
            raise ValueError("temp output was enabled for this task, thus its output is not available locallly.")

        if not self._output_loaded:
            if self.successful():
                try:
                    output = self._output_file.contents()
                    if self._serialize_output:
                        output = cloudpickle.loads(output)
                except Exception as e:
                    self._output = e

                if output['Success']:
                    self._output = output['Result']
                else:
                    self._output = output['Reason']

            else:
                self._output = FunctionCallNoResult()

            self._output_loaded = True
        return self._output

    def __del__(self):
        try:
            if self._input_file:
                self.manager.undeclare_file(self._input_file)
                self._input_file = None
            super().__del__()
        except TypeError:
            # in case the interpreter is shuting down. staging files will be deleted by manager atexit function.
            pass


class FunctionCallNoResult(Exception):
    pass


##
# \class LibraryTask
#
# TaskVine LibraryTask object
#
# This class represents a task that contains persistent Python functions at the worker
class LibraryTask(Task):
    ##
    # Create a new LibraryTask task specification.
    #
    # @param self               Reference to the current remote task object.
    # @param fn                 The command for this LibraryTask to run
    # @param library_name       The name of this Library.
    def __init__(self, fn, library_name):
        Task.__init__(self, fn)
        self._manager_will_free = True
        self.provides_library(library_name)


# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
