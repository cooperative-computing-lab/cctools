##
# @namespace ndcctools.work_queue
#
# Work Queue distributed task execution framework - Python interface.
#
# The Work Queue system provides a distributed task execution framework
# for high throughput applications consisting of millions of tasks running
# on thousands of workers.  This Python interface allows for the creation
# of manager applications that define, submit, monitor, and collect tasks.
# The objects and methods provided by this package correspond to the native
# C API in @ref work_queue.h.
#
# The SWIG-based Python bindings provide a higher-level interface that
# revolves around the following objects:
#
# - @ref ndcctools.work_queue.WorkQueue
# - @ref ndcctools.work_queue.Task
# - @ref ndcctools.work_queue.Factory

from .cwork_queue import *
from .work_queue_display import JupyterDisplay

from ndcctools.resource_monitor import (
    rmsummary_delete,
    rmsummary_create,
    rmsummaryArray_getitem,
    delete_rmsummaryArray,
)

import itertools
import copy
import os
import sys
import json
import errno
import tempfile
import subprocess
import uuid
import textwrap
import shutil
import atexit
import time
import math
import weakref


def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)


def specify_debug_log(logfile):
    set_debug_flag("all")
    cctools_debug_config_file_size(0)
    cctools_debug_config_file(logfile)


def specify_port_range(low_port, high_port):
    if low_port > high_port:
        raise TypeError("low_port {} should be smaller than high_port {}".format(low_port, high_port))

    os.environ["TCP_LOW_PORT"] = str(low_port)
    os.environ["TCP_HIGH_PORT"] = str(high_port)


cctools_debug_config("work_queue_python")
cctools_tmpdir = os.getenv("CCTOOLS_TEMP", None)

if cctools_tmpdir:
    staging_directory = tempfile.mkdtemp(prefix="wq-py-staging-", dir=cctools_tmpdir)
else:
    staging_directory = tempfile.mkdtemp(prefix="wq-py-staging-")


def cleanup_staging_directory():
    try:
        if shutil and os and staging_directory and os.path.exists(staging_directory):
            shutil.rmtree(staging_directory)
    except Exception as e:
        sys.stderr.write("could not delete {}: {}\n".format(staging_directory, e))


atexit.register(cleanup_staging_directory)


##
# \class ndcctools.work_queue.Task
#
# Python Task object
#
# This class is used to create a task specification.
class Task(object):
    ##
    # Create a new task specification.
    #
    # @param self       Reference to the current task object.
    # @param command    The shell command line to be exected by the task.
    def __init__(self, command):
        self._task = None

        self._manager = None  # set on submission

        self._task = work_queue_task_create(command)
        if not self._task:
            raise Exception("Unable to create internal Task structure")

        self._finalizer = weakref.finalize(self, self._free)

    def _free(self):
        if not self._task:
            return
        if self._manager and self._manager._finalizer.alive and self.id in self._manager._task_table:
            # interpreter is shutting down. Don't delete task here so that manager
            # does not get memory errors
            return
        work_queue_task_delete(self._task)
        self._task = None

    @staticmethod
    def _determine_file_flags(flags, cache, failure_only):
        # if flags is defined, use its value. Otherwise do not cache or failure_only only if
        # asked explicitely.

        if flags is None:
            flags = WORK_QUEUE_NOCACHE

        if cache is not None:
            if cache:
                flags = flags | WORK_QUEUE_CACHE
            else:
                flags = flags & ~(WORK_QUEUE_CACHE)

        if failure_only is not None:
            if failure_only:
                flags = flags | WORK_QUEUE_FAILURE_ONLY
            else:
                flags = flags & ~(WORK_QUEUE_FAILURE_ONLY)

        return flags

    ##
    # Return a copy of this task
    #
    def clone(self):
        """Return a (deep)copy this task that can also be submitted to the WorkQueue."""
        new = copy.copy(self)
        new._task = work_queue_task_clone(self._task)
        return new

    ##
    # Set the command to be executed by the task.
    #
    # @param self       Reference to the current task object.
    # @param command    The command to be executed.
    def specify_command(self, command):
        return work_queue_task_specify_command(self._task, command)

    ##
    # Set the coprocess at the worker that should execute the task's command.
    # This is not needed for regular tasks.
    #
    # @param self       Reference to the current task object.
    # @param coprocess  The name of the coprocess.
    def specify_coprocess(self, coprocess):
        return work_queue_task_specify_coprocess(self._task, coprocess)

    ##
    # Set the worker selection algorithm for task.
    #
    # @param self       Reference to the current task object.
    # @param algorithm  One of the following algorithms to use in assigning a
    #                   task to a worker. See @ref work_queue_schedule_t for
    #                   possible values.
    def specify_algorithm(self, algorithm):
        return work_queue_task_specify_algorithm(self._task, algorithm)

    ##
    # Attach a user defined logical name to the task.
    #
    # @param self       Reference to the current task object.
    # @param tag        The tag to attach to task.
    def specify_tag(self, tag):
        return work_queue_task_specify_tag(self._task, tag)

    ##
    # Label the task with the given category. It is expected that tasks with the
    # same category have similar resources requirements (e.g. for fast abort).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the category
    def specify_category(self, name):
        return work_queue_task_specify_category(self._task, name)

    ##
    # Label the task with the given user-defined feature. Tasks with the
    # feature will only run on workers that provide it (see worker's
    # --feature option).
    #
    # @param self       Reference to the current task object.
    # @param name       The name of the feature.
    def specify_feature(self, name):
        return work_queue_task_specify_feature(self._task, name)

    ##
    # Add a file to the task.
    #
    # @param self           Reference to the current task object.
    # @param local_name     The name of the file on local disk or shared filesystem.
    # @param remote_name    The name of the file at the execution site.
    # @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref work_queue_file_flags_t or'd together The most common are:
    #                       - @ref WORK_QUEUE_NOCACHE (default)
    #                       - @ref WORK_QUEUE_CACHE
    #                       - @ref WORK_QUEUE_WATCH
    #                       - @ref WORK_QUEUE_FAILURE_ONLY
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs).
    #
    # For example:
    # @code
    # # The following are equivalent
    # >>> task.specify_file("/etc/hosts", type=WORK_QUEUE_INPUT, cache = True)
    # >>> task.specify_file("/etc/hosts", "hosts", type=WORK_QUEUE_INPUT, cache = True)
    # @endcode
    def specify_file(self, local_name, remote_name=None, type=None, flags=None, cache=None, failure_only=None):
        # swig expects strings:
        if local_name:
            local_name = str(local_name)

        if remote_name:
            remote_name = str(remote_name)
        else:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache, failure_only)
        return work_queue_task_specify_file(self._task, local_name, remote_name, type, flags)

    ##
    # Add a url to the task which will be provided as an input file.
    #
    # @param self           Reference to the current task object.
    # @param url            The url of the file to provide.
    # @param remote_name    The name of the file as seen by the task.
    # @param type           Must be @ref WORK_QUEUE_INPUT.  (Output is not currently supported.)
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref work_queue_file_flags_t or'd together The most common are:
    #                       - @ref WORK_QUEUE_NOCACHE (default)
    #                       - @ref WORK_QUEUE_CACHE
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs).
    # @param cache         Whether the file should be cached at workers (True/False)
    #
    # For example:
    # @code
    # >>> task.specify_url("http://www.google.com/","google.txt",type=WORK_QUEUE_INPUT,flags=WORK_QUEUE_CACHE);
    # @endcode

    def specify_url(self, url, remote_name, type=None, flags=None, cache=None, failure_only=None):
        if type is None:
            type = WORK_QUEUE_INPUT

        if type == WORK_QUEUE_OUTPUT:
            raise ValueError("specify_url does not currently support output files.")

        # swig expects strings
        if remote_name:
            remote_name = str(remote_name)

        if url:
            url = str(url)

        flags = Task._determine_file_flags(flags, cache, failure_only)
        return work_queue_task_specify_url(self._task, url, remote_name, type, flags)

    ##
    # Add an input file produced by a Unix shell command.
    # The command will be executed at the worker and produce
    # a cacheable file that can be shared among multiple tasks.
    #
    # @param self           Reference to the current task object.
    # @param cmd            The shell command which will produce the file.
    #                       The command must contain the string %% which will be replaced with the cached location of the file.
    # @param remote_name    The name of the file as seen by the task.
    # @param type           Must be @ref WORK_QUEUE_INPUT.  (Output is not currently supported.)
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref work_queue_file_flags_t or'd together The most common are:
    #                       - @ref WORK_QUEUE_NOCACHE (default)
    #                       - @ref WORK_QUEUE_CACHE
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs).
    # @param cache         Whether the file should be cached at workers (True/False)
    #
    # For example:
    # @code
    # >>> task.specify_file_command("curl http://www.example.com/mydata.gz | gunzip > %%","infile",type=WORK_QUEUE_INPUT,flags=WORK_QUEUE_CACHE);
    # @endcode

    def specify_file_command(self, cmd, remote_name, type=None, flags=None, cache=None, failure_only=None):
        if type is None:
            type = WORK_QUEUE_INPUT

        if type == WORK_QUEUE_OUTPUT:
            raise ValueError("specify_file_command does not currently support output files.")

        # swig expects strings
        if remote_name:
            remote_name = str(remote_name)

        flags = Task._determine_file_flags(flags, cache, failure_only)
        return work_queue_task_specify_file_command(self._task, cmd, remote_name, type, flags)

    ##
    # Add a file piece to the task.
    #
    # @param self           Reference to the current task object.
    # @param local_name     The name of the file on local disk or shared filesystem.
    # @param remote_name    The name of the file at the execution site.
    # @param start_byte     The starting byte offset of the file piece to be transferred.
    # @param end_byte       The ending byte offset of the file piece to be transferred.
    # @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref work_queue_file_flags_t or'd together The most common are:
    #                       - @ref WORK_QUEUE_NOCACHE (default)
    #                       - @ref WORK_QUEUE_CACHE
    #                       - @ref WORK_QUEUE_FAILURE_ONLY
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs).
    def specify_file_piece(self, local_name, remote_name=None, start_byte=0, end_byte=0, type=None, flags=None, cache=None, failure_only=None):
        if local_name:
            local_name = str(local_name)

        if remote_name:
            remote_name = str(remote_name)
        else:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache, failure_only)
        return work_queue_task_specify_file_piece(self._task, local_name, remote_name, start_byte, end_byte, type, flags)

    ##
    # Add a input file to the task.
    #
    # This is just a wrapper for @ref ndcctools.work_queue.Task.specify_file with type set to @ref WORK_QUEUE_INPUT.
    def specify_input_file(self, local_name, remote_name=None, flags=None, cache=None):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_INPUT, flags, cache, failure_only=None)

    ##
    # Add a output file to the task.
    #
    # This is just a wrapper for @ref ndcctools.work_queue.Task.specify_file with type set to @ref WORK_QUEUE_OUTPUT.
    def specify_output_file(self, local_name, remote_name=None, flags=None, cache=None, failure_only=None):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_OUTPUT, flags, cache, failure_only)

    ##
    # Add a directory to the task.
    # @param self           Reference to the current task object.
    # @param local_name     The name of the directory on local disk or shared filesystem. Optional if the directory is empty.
    # @param remote_name    The name of the directory at the remote execution site.
    # @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref work_queue_file_flags_t or'd together The most common are:
    #                       - @ref WORK_QUEUE_NOCACHE
    #                       - @ref WORK_QUEUE_CACHE
    # @param recursive      Indicates whether just the directory (False) or the directory and all of its contents (True) should be included.
    #                       - @ref WORK_QUEUE_FAILURE_ONLY
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param failure_only  For output directories, whether the file should be retrieved only when the task fails (e.g., debug logs).
    # @return 1 if the task directory is successfully specified, 0 if either of @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
    def specify_directory(self, local_name, remote_name=None, type=None, flags=None, recursive=False, cache=None, failure_only=None):
        if local_name:
            local_name = str(local_name)

        if remote_name:
            remote_name = str(remote_name)
        else:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache, failure_only)
        return work_queue_task_specify_directory(self._task, local_name, remote_name, type, flags, recursive)

    ##
    # Add an input bufer to the task.
    #
    # @param self           Reference to the current task object.
    # @param buffer         The contents of the buffer to pass as input.
    # @param remote_name    The name of the remote file to create.
    # @param flags          May take the same values as @ref ndcctools.work_queue.Task.specify_file.
    # @param cache          Whether the file should be cached at workers (True/False)
    def specify_buffer(self, buffer, remote_name, flags=None, cache=None):
        if remote_name:
            remote_name = str(remote_name)
        flags = Task._determine_file_flags(flags, cache, None)
        return work_queue_task_specify_buffer(self._task, buffer, len(buffer), remote_name, flags)

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
    def specify_snapshot_file(self, filename):
        return work_queue_specify_snapshot_file(self._task, filename)

    ##
    # Indicate the number of times the task should be retried. If 0 (the
    # default), the task is tried indefinitely. A task that did not succeed
    # after the given number of retries is returned with result
    # WORK_QUEUE_RESULT_MAX_RETRIES.
    def specify_max_retries(self, max_retries):
        return work_queue_task_specify_max_retries(self._task, max_retries)

    ##
    # Indicate the number of cores required by this task.
    def specify_cores(self, cores):
        return work_queue_task_specify_cores(self._task, cores)

    ##
    # Indicate the memory (in MB) required by this task.
    def specify_memory(self, memory):
        return work_queue_task_specify_memory(self._task, memory)

    ##
    # Indicate the disk space (in MB) required by this task.
    def specify_disk(self, disk):
        return work_queue_task_specify_disk(self._task, disk)

    ##
    # Indicate the number of GPUs required by this task.
    def specify_gpus(self, gpus):
        return work_queue_task_specify_gpus(self._task, gpus)

    ##
    # Indicate the the priority of this task (larger means better priority, default is 0).
    def specify_priority(self, priority):
        return work_queue_task_specify_priority(self._task, priority)

    # Indicate the maximum end time (absolute, in microseconds from the Epoch) of this task.
    # This is useful, for example, when the task uses certificates that expire.
    # If less than 1, or not specified, no limit is imposed.
    def specify_end_time(self, useconds):
        return work_queue_task_specify_end_time(self._task, int(useconds))

    # Indicate the minimum start time (absolute, in microseconds from the Epoch) of this task.
    # If less than 1, or not specified, no limit is imposed.
    def specify_start_time_min(self, useconds):
        return work_queue_task_specify_start_time_min(self._task, int(useconds))

    # Indicate the maximum running time (in microseconds) for a task in a
    # worker (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    # Note: It has the same effect that ndcctools.work_queue.WorkQueue.specify_running_time_max, but specified
    # in microseconds. Kept for backwards compatibility.
    def specify_running_time(self, useconds):
        return work_queue_task_specify_running_time(self._task, int(useconds))

    # Indicate the maximum running time (in seconds) for a task in a worker
    # (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def specify_running_time_max(self, seconds):
        return work_queue_task_specify_running_time_max(self._task, int(seconds))

    # Indicate the minimum running time (in seconds) for a task in a worker
    # (relative to when the task starts to run).  If less than 1, or not
    # specified, no limit is imposed.
    def specify_running_time_min(self, seconds):
        return work_queue_task_specify_running_time_min(self._task, int(seconds))

    ##
    # Set this environment variable before running the task.
    # If value is None, then variable is unset.
    def specify_environment_variable(self, name, value=None):
        return work_queue_task_specify_environment_variable(self._task, name, value)

    ##
    # Set a name for the resource summary output directory from the monitor.
    def specify_monitor_output(self, directory):
        return work_queue_task_specify_monitor_output(self._task, directory)

    ##
    # Get the user-defined logical name for the task.
    #
    # @code
    # >>> print(t.tag)
    # @endcode
    @property
    def tag(self):
        return self._task.tag

    ##
    # Get the category name for the task.
    #
    # @code
    # >>> print(t.category)
    # @endcode
    @property
    def category(self):
        return self._task.category

    ##
    # Get the shell command executed by the task.
    # @code
    # >>> print(t.command)
    # @endcode
    @property
    def command(self):
        return self._task.command_line

    ##
    # Get the priority of the task.
    # @code
    # >>> print(t.priority)
    # @endcode
    @property
    def priority(self):
        return self._task.priority

    ##
    # Get the algorithm for choosing worker to run the task.
    # @code
    # >>> print(t.algorithm)
    # @endcode
    @property
    def algorithm(self):
        return self._task.worker_selection_algorithm

    ##
    # Get the standard output of the task. Must be called only after the task
    # completes execution.
    # @code
    # >>> print(t.std_output)
    # @endcode
    @property
    def std_output(self):
        return self._task.output  # note we use .output, see below.)

    ##
    # Get the standard output of the task. (Same as t.std_output for regular
    # work queue tasks) Must be called only after the task completes execution.
    # @code
    # >>> print(t.output)
    # @endcode
    @property
    def output(self):
        return self._task.output

    ##
    # Get the task id number. Must be called only after the task was submitted.
    # @code
    # >>> print(t.id)
    # @endcode
    @property
    def id(self):
        return self._task.taskid

    ##
    # Get the exit code of the command executed by the task. Must be called only
    # after the task completes execution.
    # @code
    # >>> print(t.return_status)
    # @endcode
    @property
    def return_status(self):
        return self._task.return_status

    ##
    # Get the result of the task as an integer code, such as successful, missing file, etc.
    # See @ref work_queue_result_t for possible values.  Must be called only
    # after the task completes execution.
    # @code
    # >>> print(t.result)
    # 0
    # @endcode
    @property
    def result(self):
        return self._task.result

    ##
    # Return a string that explains the result of a task.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.result_str)
    # 'SUCCESS'
    # @endcode
    @property
    def result_str(self):
        return work_queue_result_str(self._task.result)

    ##
    # Get the number of times the task has been resubmitted internally.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_submissions)
    # @endcode
    @property
    def total_submissions(self):
        return self._task.total_submissions

    ##
    # Get the number of times the task has been failed given resource exhaustion.
    # @code
    # >>> print(t.exhausted_attempts)
    # @endcode
    @property
    def exhausted_attempts(self):
        return self._task.exhausted_attempts

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.host)
    # @endcode
    @property
    def host(self):
        return self._task.host

    ##
    # Get the name of the host on which the task ran.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.hostname)
    # @endcode
    @property
    def hostname(self):
        return self._task.hostname

    ##
    # Get the time at which this task was submitted.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.submit_time)
    # @endcode
    @property
    def submit_time(self):
        return self._task.time_task_submit

    ##
    # Get the time at which this task was finished.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.finish_time)
    # @endcode
    @property
    def finish_time(self):
        return self._task.time_task_finish

    ##
    # Get the total time the task executed and failed given resource exhaustion.
    # @code
    # >>> print(t.total_cmd_exhausted_execute_time)
    # @endcode
    @property
    def total_cmd_exhausted_execute_time(self):
        return self._task.total_cmd_exhausted_execute_time

    ##
    # Get the time spent in upper-level application (outside of work_queue_wait).
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.app_delay)
    # @endcode
    @property
    def app_delay(self):
        return self._task.time_app_delay

    ##
    # Get the time at which the task started to transfer input files.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.send_input_start)
    # @endcode
    @property
    def send_input_start(self):
        return self._task.time_send_input_start

    ##
    # Get the time at which the task finished transferring input files.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.send_input_finish)
    # @endcode
    @property
    def send_input_finish(self):
        return self._task.time_send_input_finish

    ##
    # The time at which the task began.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.execute_cmd_start)
    # @endcode
    @property
    def execute_cmd_start(self):
        return self._task.time_execute_cmd_start

    ##
    # Get the time at which the task finished (discovered by the manager).
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.execute_cmd_finish)
    # @endcode
    @property
    def execute_cmd_finish(self):
        return self._task.time_execute_cmd_finish

    ##
    # Get the time at which the task started to transfer output files.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.receive_output_start)
    # @endcode
    @property
    def receive_output_start(self):
        return self._task.time_receive_output_start

    ##
    # Get the time at which the task finished transferring output files.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.receive_output_finish)
    # @endcode
    @property
    def receive_output_finish(self):
        return self._task.time_receive_output_finish

    ##
    # Get the number of bytes received since task started receiving input data.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_bytes_received)
    # @endcode
    @property
    def total_bytes_received(self):
        return self._task.total_bytes_received

    ##
    # Get the number of bytes sent since task started sending input data.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_bytes_sent)
    # @endcode
    @property
    def total_bytes_sent(self):
        return self._task.total_bytes_sent

    ##
    # Get the number of bytes transferred since task started transferring input data.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_bytes_transferred)
    # @endcode
    @property
    def total_bytes_transferred(self):
        return self._task.total_bytes_transferred

    ##
    # Get the time comsumed in microseconds for transferring total_bytes_transferred.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_transfer_time)
    # @endcode
    @property
    def total_transfer_time(self):
        return self._task.total_transfer_time

    ##
    # Time spent in microseconds for executing the command until completion on a single worker.
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.cmd_execution_time)
    # @endcode
    @property
    def cmd_execution_time(self):
        return self._task.cmd_execution_time

    ##
    # Accumulated time spent in microseconds for executing the command on any
    # worker, regardless of whether the task finished (i.e., this includes time
    # running on workers that disconnected).
    #
    # Must be called only after the task completes execution.
    # @code
    # >>> print(t.total_cmd_execution_time)
    # @endcode
    @property
    def total_cmd_execution_time(self):
        return self._task.total_cmd_execution_time

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
    # Get the resources the task exceeded. For valid field see @ref ndcctools.work_queue.Task.resources_measured.
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
    # @ref ndcctools.work_queue.Task.resources_measured.
    #
    @property
    def resources_requested(self):
        if not self._task.resources_requested:
            return None
        return self._task.resources_requested

    ##
    # Get the resources allocated to the task in its latest attempt. For valid
    # fields see @ref ndcctools.work_queue.Task.resources_measured.
    #
    @property
    def resources_allocated(self):
        if not self._task.resources_allocated:
            return None
        return self._task.resources_allocated


##
# \class PythonTask
#
# Python PythonTask object
#
# this class is used to create a python task
try:
    import cloudpickle

    pythontask_available = True
except Exception:
    # Note that the intended exception here is ModuleNotFoundError.
    # However, that type does not exist in Python 2
    pythontask_available = False


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
        self._tmpdir = tempfile.mkdtemp(dir=staging_directory)

        if not pythontask_available:
            raise RuntimeError("PythonTask is not available. The cloudpickle module is missing.")

        self._func_file = os.path.join(self._tmpdir, "function_{}.p".format(self._id))
        self._args_file = os.path.join(self._tmpdir, "args_{}.p".format(self._id))
        self._out_file = os.path.join(self._tmpdir, "out_{}.p".format(self._id))
        self._wrapper = os.path.join(self._tmpdir, "pytask_wrapper_{}.py".format(self._id))

        self._pp_run = None
        self._env_file = None

        self._serialize_python_function(func, args, kwargs)
        self._create_wrapper()

        self._command = self._python_function_command()

        self._output_loaded = False
        self._output = None

        super(PythonTask, self).__init__(self._command)
        self._specify_IO_files()

        self._finalizer = weakref.finalize(self, self._free)

    def _free(self):
        if self._tmpdir and os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir)
        super()._free()

    ##
    # returns the result of a python task as a python variable
    #
    # @param self	reference to the current python task object
    @property
    def output(self):
        if not self._output_loaded:
            if self.result == WORK_QUEUE_RESULT_SUCCESS:
                try:
                    with open(os.path.join(self._tmpdir, "out_{}.p".format(self._id)), "rb") as f:
                        self._output = cloudpickle.load(f)
                except Exception as e:
                    self._output = e
            else:
                self._output = PythonTaskNoResult()
                print(self.std_output)
            self._output_loaded = True
        return self._output

    def specify_environment(self, env_file):
        if env_file:
            self._env_file = env_file
            self._pp_run = shutil.which("poncho_package_run")

            if not self._pp_run:
                raise RuntimeError("Could not find poncho_package_run in PATH.")

            self._command = self._python_function_command()
            work_queue_task_specify_command(self._task, self._command)

            self.specify_input_file(self._env_file, cache=True)
            self.specify_input_file(self._pp_run, cache=True)

    specify_package = specify_environment

    def _serialize_python_function(self, func, args, kwargs):
        with open(self._func_file, "wb") as wf:
            cloudpickle.dump(func, wf)
        with open(self._args_file, "wb") as wf:
            cloudpickle.dump([args, kwargs], wf)

    def _python_function_command(self):
        if self._env_file:
            py_exec = "python"
        else:
            py_exec = f"python{sys.version_info[0]}"

        command = "{py_exec} {wrapper} {function} {args} {out}".format(
            py_exec=py_exec,
            wrapper=os.path.basename(self._wrapper),
            function=os.path.basename(self._func_file),
            args=os.path.basename(self._args_file),
            out=os.path.basename(self._out_file),
        )

        if self._env_file:
            command = './{pprun} -e {tar} --unpack-to "$WORK_QUEUE_SANDBOX"/{unpack}-env {cmd}'.format(
                pprun=os.path.basename(self._pp_run),
                unpack=os.path.basename(self._env_file),
                tar=os.path.basename(self._env_file),
                cmd=command,
            )

        return command

    def _specify_IO_files(self):
        self.specify_input_file(self._wrapper, cache=True)
        self.specify_input_file(self._func_file, cache=False)
        self.specify_input_file(self._args_file, cache=False)
        self.specify_output_file(self._out_file, cache=False)

    ##
    # creates the wrapper script which will execute the function. pickles output.
    def _create_wrapper(self):
        with open(self._wrapper, "w") as f:
            f.write(
                textwrap.dedent(
                    """\
                try:
                    import sys
                    import cloudpickle
                except ImportError:
                    print("Could not execute PythonTask function because a needed module for Work Queue was not available.")
                    raise

                (fn, args, out) = sys.argv[1], sys.argv[2], sys.argv[3]
                with open (fn , 'rb') as f:
                    exec_function = cloudpickle.load(f)
                with open(args, 'rb') as f:
                    args, kwargs = cloudpickle.load(f)
                try:
                    exec_out = exec_function(*args, **kwargs)

                except Exception as e:
                    exec_out = e

                with open(out, 'wb') as f:
                    cloudpickle.dump(exec_out, f)

                print(exec_out)"""
                )
            )


class PythonTaskNoResult(Exception):
    pass


##
# Python Work Queue object
#
# @class WorkQueue
class WorkQueue(object):
    ##
    # Create a new work queue.
    #
    # @param self       Reference to the current work queue object.
    # @param port       The port number to listen on. If zero, then a random port is chosen. A range of possible ports (low, hight) can be also specified instead of a single integer.
    # @param name       The project name to use.
    # @param stats_log  The name of a file to write the queue's statistics log.
    # @param transactions_log  The name of a file to write the queue's transactions log.
    # @param debug_log  The name of a file to write the queue's debug log.
    # @param shutdown   Automatically shutdown workers when queue is finished. Disabled by default.
    # @param ssl        A tuple of filenames (ssl_key, ssl_cert) in pem format, or True.
    #                   If not given, then TSL is not activated. If True, a self-signed temporary key and cert are generated.
    # @param status_display_interval Number of seconds between updates to the jupyter status display. None, or less than 1 disables it.
    #
    # @see work_queue_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=WORK_QUEUE_DEFAULT_PORT, name=None, shutdown=False, stats_log=None, transactions_log=None, debug_log=None, ssl=None, status_display_interval=None):
        self._shutdown = shutdown
        self._work_queue = None
        self._stats = None
        self._stats_hierarchy = None
        self._task_table = {}
        self._info_widget = None
        self._using_ssl = False

        # if we were given a range ports, rather than a single port to try.
        lower, upper = None, None
        try:
            lower, upper = port
            specify_port_range(lower, upper)
            port = 0
        except TypeError:
            # if not a range, ignore
            pass
        except ValueError:
            raise ValueError("port should be a single integer, or a sequence of two integers")

        if status_display_interval and status_display_interval >= 1:
            self._info_widget = JupyterDisplay(interval=status_display_interval)

        try:
            if debug_log:
                specify_debug_log(debug_log)
            self._stats = work_queue_stats()
            self._stats_hierarchy = work_queue_stats()

            ssl_key, ssl_cert = self._setup_ssl(ssl)
            self._work_queue = work_queue_ssl_create(port, ssl_key, ssl_cert)

            if ssl_key:
                self._using_ssl = True

            if not self._work_queue:
                raise Exception("Could not create queue on port {}".format(port))

            if stats_log:
                self.specify_log(stats_log)

            if transactions_log:
                self.specify_transactions_log(transactions_log)

            if name:
                work_queue_specify_name(self._work_queue, name)
        except Exception as e:
            raise Exception("Unable to create internal Work Queue structure: {}".format(e))
        finally:
            self._finalizer = weakref.finalize(self, self._free)
        self._update_status_display()

    def _free(self):
        if self._work_queue:
            if self._shutdown:
                self.shutdown_workers(0)
            self._update_status_display(force=True)
            work_queue_delete(self._work_queue)
            self._work_queue = None

    def __del__(self):
        # method needed because coffea executor pre-2023 calls __del__ explicitly.
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._update_status_display(force=True)
        self._finalizer()

    def _setup_ssl(self, ssl):
        if not ssl:
            return (None, None)

        if ssl is not True:
            return ssl

        (tmp, key) = tempfile.mkstemp(dir=staging_directory, prefix="key")
        os.close(tmp)
        (tmp, cert) = tempfile.mkstemp(dir=staging_directory, prefix="cert")
        os.close(tmp)

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
            print(f"status display error {e}", file=sys.stderr)

    ##
    # Get the project name of the queue.
    # @code
    # >>> print(q.name)
    # @endcode
    @property
    def name(self):
        return work_queue_name(self._work_queue)

    ##
    # Get the listening port of the queue.
    # @code
    # >>> print(q.port)
    # @endcode
    @property
    def port(self):
        return work_queue_port(self._work_queue)

    ##
    # Whether the manager is using ssl to talk to workers
    # @code
    # >>> print(q.using_ssl)
    # @endcode
    @property
    def using_ssl(self):
        return self._using_ssl

    ##
    # Get queue statistics.
    # @code
    # >>> print(q.stats)
    # @endcode
    # The fields in @ref ndcctools.work_queue.WorkQueue.stats can also be individually accessed through this call. For example:
    # @code
    # >>> print(q.stats.workers_busy)
    # @endcode
    @property
    def stats(self):
        work_queue_get_stats(self._work_queue, self._stats)
        return self._stats

    ##
    # Get worker hierarchy statistics.
    # @code
    # >>> print(q.stats_hierarchy)
    # @endcode
    # The fields in @ref ndcctools.work_queue.WorkQueue.stats_hierarchy can also be individually accessed through this call. For example:
    # @code
    # >>> print(q.stats_hierarchy.workers_busy)
    # @endcode
    @property
    def stats_hierarchy(self):
        work_queue_get_stats_hierarchy(self._work_queue, self._stats_hierarchy)
        return self._stats_hierarchy

    ##
    # Get the task statistics for the given category.
    #
    # @param self   Reference to the current work queue object.
    # @param category   A category name.
    # For example:
    # @code
    # s = q.stats_category("my_category")
    # >>> print(s)
    # @endcode
    # The fields in @ref work_queue_stats can also be individually accessed through this call. For example:
    # @code
    # >>> print(s.tasks_waiting)
    # @endcode
    def stats_category(self, category):
        stats = work_queue_stats()
        work_queue_get_stats_category(self._work_queue, category, stats)
        return stats

    ##
    # Get queue information as list of dictionaries
    # @param self Reference to the current work queue object
    # @param request One of: "queue", "tasks", "workers", or "categories"
    # For example:
    # @code
    # import json
    # tasks_info = q.status("tasks")
    # @endcode
    def status(self, request):
        info_raw = work_queue_status(self._work_queue, request)
        info_json = json.loads(info_raw)
        del info_raw
        return info_json

    ##
    # Get resource statistics of workers connected.
    #
    # @param self 	Reference to the current work queue object.
    # @return A list of dictionaries that indicate how many .workers
    # connected with a certain number of .cores, .memory, and disk.
    # For example:
    # @code
    # workers = q.worker_summary()
    # >>> for w in workers:
    # >>>    print("{} workers with: {} cores, {} MB memory, {} MB disk".format(w.workers, w.cores, w.memory, w.disk)
    # @endcode
    def workers_summary(self):
        from_c = work_queue_workers_summary(self._work_queue)

        count = 0
        workers = []
        while True:
            s = rmsummaryArray_getitem(from_c, count)
            if not s:
                break
            workers.append(
                {
                    "workers": int(s.workers),
                    "cores": int(s.cores),
                    "gpus": int(s.gpus),
                    "memory": int(s.memory),
                    "disk": int(s.disk),
                }
            )
            rmsummary_delete(s)
            count += 1
        delete_rmsummaryArray(from_c)
        return workers

    ##
    # Turn on or off first-allocation labeling for a given category. By
    # default, only cores, memory, and disk are labeled, and gpus are unlabeled.
    # NOTE: autolabeling is only meaningfull when task monitoring is enabled
    # (@ref ndcctools.work_queue.WorkQueue.enable_monitoring). When monitoring is enabled and a task exhausts
    # resources in a worker, mode dictates how work queue handles the
    # exhaustion:
    # @param self Reference to the current work queue object.
    # @param category A category name. If None, sets the mode by default for
    # newly created categories.
    # @param mode One of:
    #                  - WORK_QUEUE_ALLOCATION_MODE_FIXED Task fails (default).
    #                  - WORK_QUEUE_ALLOCATION_MODE_MAX If maximum values are
    #                  specified for cores, memory, disk, and gpus (e.g. via @ref
    #                  ndcctools.work_queue.WorkQueue.specify_category_max_resources or @ref ndcctools.work_queue.Task.specify_memory),
    #                  and one of those resources is exceeded, the task fails.
    #                  Otherwise it is retried until a large enough worker
    #                  connects to the manager, using the maximum values
    #                  specified, and the maximum values so far seen for
    #                  resources not specified. Use @ref ndcctools.work_queue.Task.specify_max_retries to
    #                  set a limit on the number of times work queue attemps
    #                  to complete the task.
    #                  - WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE As above, but
    #                  work queue tries allocations to minimize resource waste.
    #                  - WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT As above, but
    #                  work queue tries allocations to maximize throughput.
    def specify_category_mode(self, category, mode):
        return work_queue_specify_category_mode(self._work_queue, category, mode)

    ##
    # Turn on or off first-allocation labeling for a given category and
    # resource. This function should be use to fine-tune the defaults from @ref
    # ndcctools.work_queue.WorkQueue.specify_category_mode.
    # @param self   Reference to the current work queue object.
    # @param category A category name.
    # @param resource A resource name.
    # @param autolabel True/False for on/off.
    # @returns 1 if resource is valid, 0 otherwise.
    def specify_category_autolabel_resource(self, category, resource, autolabel):
        return work_queue_enable_category_resource(self._work_queue, category, category, resource, autolabel)

    ##
    # Get current task state. See @ref work_queue_task_state_t for possible values.
    # @code
    # >>> print(q.task_state(taskid))
    # @endcode
    def task_state(self, taskid):
        return work_queue_task_state(self._work_queue, taskid)

    ##
    # Enables resource monitoring of tasks in the queue, and writes a summary
    # per task to the directory given. Additionally, all summaries are
    # consolidate into the file all_summaries-PID.log
    #
    #  Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self   Reference to the current work queue object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring(self, dirname=None, watchdog=True):
        return work_queue_enable_monitoring(self._work_queue, dirname, watchdog)

    ##
    # As @ref ndcctools.work_queue.WorkQueue.enable_monitoring, but it also generates a time series and a debug file.
    # WARNING: Such files may reach gigabyte sizes for long running tasks.
    #
    # Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self   Reference to the current work queue object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring_full(self, dirname=None, watchdog=True):
        return work_queue_enable_monitoring_full(self._work_queue, dirname, watchdog)

    ##
    # Turn on or off fast abort functionality for a given queue for tasks in
    # the "default" category, and for task which category does not set an
    # explicit multiplier.
    #
    # @param self       Reference to the current work queue object.
    # @param multiplier The multiplier of the average task time at which point to abort; if negative (the default) fast_abort is deactivated.
    def activate_fast_abort(self, multiplier):
        return work_queue_activate_fast_abort(self._work_queue, multiplier)

    ##
    # Turn on or off fast abort functionality for a given queue.
    #
    # @param self       Reference to the current work queue object.
    # @param name       Name of the category.
    # @param multiplier The multiplier of the average task time at which point to abort; if zero, deacticate for the category, negative (the default), use the one for the "default" category (see @ref ndcctools.work_queue.WorkQueue.activate_fast_abort)
    def activate_fast_abort_category(self, name, multiplier):
        return work_queue_activate_fast_abort_category(self._work_queue, name, multiplier)

    ##
    # Turn on or off draining mode for workers at hostname.
    #
    # @param self       Reference to the current work queue object.
    # @param hostname   The hostname the host running the workers.
    # @param drain_mode If True, no new tasks are dispatched to workers at hostname, and empty workers are shutdown. Else, workers works as usual.
    def specify_draining_by_hostname(self, hostname, drain_mode=True):
        return work_queue_specify_draining_by_hostname(self._work_queue, hostname, drain_mode)

    ##
    # Determine whether there are any known tasks queued, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current work queue object.
    def empty(self):
        return work_queue_empty(self._work_queue)

    ##
    # Determine whether the queue can support more tasks.
    #
    # Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
    #
    # @param self       Reference to the current work queue object.
    def hungry(self):
        return work_queue_hungry(self._work_queue)

    ##
    # Set the worker selection algorithm for queue.
    #
    # @param self       Reference to the current work queue object.
    # @param algorithm  One of the following algorithms to use in assigning a
    #                   task to a worker. See @ref work_queue_schedule_t for
    #                   possible values.
    def specify_algorithm(self, algorithm):
        return work_queue_specify_algorithm(self._work_queue, algorithm)

    ##
    # Set the order for dispatching submitted tasks in the queue.
    #
    # @param self       Reference to the current work queue object.
    # @param order      One of the following algorithms to use in dispatching
    #                   submitted tasks to workers:
    #                   - @ref WORK_QUEUE_TASK_ORDER_FIFO
    #                   - @ref WORK_QUEUE_TASK_ORDER_LIFO
    def specify_task_order(self, order):
        return work_queue_specify_task_order(self._work_queue, order)

    ##
    # Change the project name for the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param name   The new project name.
    def specify_name(self, name):
        return work_queue_specify_name(self._work_queue, name)

    ##
    # Set the preference for using hostname over IP address to connect.
    # 'by_ip' uses IP addresses from the network interfaces of the manager
    # (standard behavior), 'by_hostname' to use the hostname at the manager, or
    # 'by_apparent_ip' to use the address of the manager as seen by the catalog
    # server.
    #
    # @param self Reference to the current work queue object.
    # @param mode An string to indicate using 'by_ip', 'by_hostname' or 'by_apparent_ip'.
    def specify_manager_preferred_connection(self, mode):
        return work_queue_manager_preferred_connection(self._work_queue, mode)

    ##
    # See ndcctools.work_queue.WorkQueue.specify_manager_preferred_connection
    def specify_master_preferred_connection(self, mode):
        return work_queue_manager_preferred_connection(self._work_queue, mode)

    ##
    # Set the minimum taskid of future submitted tasks.
    #
    # Further submitted tasks are guaranteed to have a taskid larger or equal
    # to minid.  This function is useful to make taskids consistent in a
    # workflow that consists of sequential managers. (Note: This function is
    # rarely used).  If the minimum id provided is smaller than the last taskid
    # computed, the minimum id provided is ignored.
    #
    # @param self   Reference to the current work queue object.
    # @param minid  Minimum desired taskid
    # @return Returns the actual minimum taskid for future tasks.
    def specify_min_taskid(self, minid):
        return work_queue_specify_min_taskid(self._work_queue, minid)

    ##
    # Change the project priority for the given queue.
    #
    # @param self       Reference to the current work queue object.
    # @param priority   An integer that presents the priorty of this work queue manager. The higher the value, the higher the priority.
    def specify_priority(self, priority):
        return work_queue_specify_priority(self._work_queue, priority)

    ##
    # Specify the number of tasks not yet submitted to the queue.
    # It is used by work_queue_factory to determine the number of workers to launch.
    # If not specified, it defaults to 0.
    # work_queue_factory considers the number of tasks as:
    # num tasks left + num tasks running + num tasks read.
    # @param self   Reference to the current work queue object.
    # @param ntasks Number of tasks yet to be submitted.
    def specify_num_tasks_left(self, ntasks):
        return work_queue_specify_num_tasks_left(self._work_queue, ntasks)

    ##
    # Specify the manager mode for the given queue.
    # (Kept for compatibility. It is no-op.)
    #
    # @param self   Reference to the current work queue object.
    # @param mode   This may be one of the following values: WORK_QUEUE_MASTER_MODE_STANDALONE or WORK_QUEUE_MASTER_MODE_CATALOG.
    def specify_manager_mode(self, mode):
        return work_queue_specify_manager_mode(self._work_queue, mode)

    ##
    # @see ndcctools.work_queue.WorkQueue.specify_manager_mode
    def specify_master_mode(self, mode):
        return work_queue_specify_manager_mode(self._work_queue, mode)

    ##
    # Specify the catalog server the manager should report to.
    #
    # @param self       Reference to the current work queue object.
    # @param hostname   The hostname of the catalog server.
    # @param port       The port the catalog server is listening on.
    def specify_catalog_server(self, hostname, port):
        return work_queue_specify_catalog_server(self._work_queue, hostname, port)

    ##
    # Specify a log file that records the cummulative stats of connected workers and submitted tasks.
    #
    # @param self     Reference to the current work queue object.
    # @param logfile  Filename.
    def specify_log(self, logfile):
        return work_queue_specify_log(self._work_queue, logfile)

    ##
    # Specify a log file that records the states of tasks.
    #
    # @param self     Reference to the current work queue object.
    # @param logfile  Filename.
    def specify_transactions_log(self, logfile):
        work_queue_specify_transactions_log(self._work_queue, logfile)

    ##
    # Add a mandatory password that each worker must present.
    #
    # @param self      Reference to the current work queue object.
    # @param password  The password.
    def specify_password(self, password):
        return work_queue_specify_password(self._work_queue, password)

    ##
    # Add a mandatory password file that each worker must present.
    #
    # @param self      Reference to the current work queue object.
    # @param file      Name of the file containing the password.

    def specify_password_file(self, file):
        return work_queue_specify_password_file(self._work_queue, file)

    ##
    #
    # Specifies the maximum resources allowed for the default category.
    # @param self      Reference to the current work queue object.
    # @param rmd       Dictionary indicating maximum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A maximum of 4 cores is found on any worker:
    # >>> q.specify_max_resources({'cores': 4})
    # >>> # A maximum of 8 cores, 1GB of memory, and 10GB disk are found on any worker:
    # >>> q.specify_max_resources({'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def specify_max_resources(self, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return work_queue_specify_max_resources(self._work_queue, rm)

    ##
    #
    # Specifies the minimum resources allowed for the default category.
    # @param self      Reference to the current work queue object.
    # @param rmd       Dictionary indicating minimum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A minimum of 2 cores is found on any worker:
    # >>> q.specify_min_resources({'cores': 2})
    # >>> # A minimum of 4 cores, 512MB of memory, and 1GB disk are found on any worker:
    # >>> q.specify_min_resources({'cores': 4, 'memory':  512, 'disk': 1024})
    # @endcode

    def specify_min_resources(self, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return work_queue_specify_min_resources(self._work_queue, rm)

    ##
    # Specifies the maximum resources allowed for the given category.
    #
    # @param self      Reference to the current work queue object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating maximum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A maximum of 4 cores may be used by a task in the category:
    # >>> q.specify_category_max_resources("my_category", {'cores': 4})
    # >>> # A maximum of 8 cores, 1GB of memory, and 10GB may be used by a task:
    # >>> q.specify_category_max_resources("my_category", {'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def specify_category_max_resources(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return work_queue_specify_category_max_resources(self._work_queue, category, rm)

    ##
    # Specifies the minimum resources allowed for the given category.
    #
    # @param self      Reference to the current work queue object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating minimum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A minimum of 2 cores is found on any worker:
    # >>> q.specify_category_min_resources("my_category", {'cores': 2})
    # >>> # A minimum of 4 cores, 512MB of memory, and 1GB disk are found on any worker:
    # >>> q.specify_category_min_resources("my_category", {'cores': 4, 'memory':  512, 'disk': 1024})
    # @endcode

    def specify_category_min_resources(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return work_queue_specify_category_min_resources(self._work_queue, category, rm)

    ##
    # Specifies the first-allocation guess for the given category
    #
    # @param self      Reference to the current work queue object.
    # @param category  Name of the category.
    # @param rmd       Dictionary indicating maximum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # For example:
    # @code
    # >>> # Tasks are first tried with 4 cores:
    # >>> q.specify_category_first_allocation_guess("my_category", {'cores': 4})
    # >>> # Tasks are first tried with 8 cores, 1GB of memory, and 10GB:
    # >>> q.specify_category_first_allocation_guess("my_category", {'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def specify_category_first_allocation_guess(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return work_queue_specify_category_first_allocation_guess(self._work_queue, category, rm)

    ##
    # Initialize first value of categories
    #
    # @param self     Reference to the current work queue object.
    # @param rm       Dictionary indicating maximum values. See @ref ndcctools.work_queue.Task.resources_measured for possible fields.
    # @param filename JSON file with resource summaries.

    def initialize_categories(self, filename, rm):
        return work_queue_initialize_categories(self._work_queue, rm, filename)

    ##
    # Cancel task identified by its taskid and remove from the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param id     The taskid returned from @ref ndcctools.work_queue.WorkQueue.submit.
    def cancel_by_taskid(self, id):
        task = None
        task_pointer = work_queue_cancel_by_taskid(self._work_queue, id)
        if task_pointer:
            task = self._task_table.pop(int(task_pointer.taskid))
        return task

    ##
    # Cancel task identified by its tag and remove from the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param tag    The tag assigned to task using @ref ndcctools.work_queue.Task.specify_tag.
    def cancel_by_tasktag(self, tag):
        task = None
        task_pointer = work_queue_cancel_by_tasktag(self._work_queue, tag)
        if task_pointer:
            task = self._task_table.pop(int(task_pointer.taskid))
        return task

    ##
    # Cancel all tasks of the given category and remove them from the queue.
    #
    # @param self   Reference to the current work queue object.
    # @param category The name of the category to cancel.
    def cancel_by_category(self, category):
        canceled_tasks = []
        ids_to_cancel = []

        for task in self._task_table.values():
            if task.category == category:
                ids_to_cancel.append(task.id)

        canceled_tasks = [self.cancel_by_taskid(id) for id in ids_to_cancel]
        return canceled_tasks

    ##
    # Shutdown workers connected to queue.
    #
    # Gives a best effort and then returns the number of workers given the shutdown order.
    #
    # @param self   Reference to the current work queue object.
    # @param n      The number to shutdown.  To shut down all workers, specify "0".
    def shutdown_workers(self, n):
        return work_queue_shut_down_workers(self._work_queue, n)

    ##
    # Block workers running on host from working for the manager.
    #
    # @param self   Reference to the current work queue object.
    # @param host   The hostname the host running the workers.
    def block_host(self, host):
        return work_queue_block_host(self._work_queue, host)

    ##
    # Replaced by @ref ndcctools.work_queue.WorkQueue.block_host
    def blacklist(self, host):
        return self.block_host(host)

    ##
    # Block workers running on host for the duration of the given timeout.
    #
    # @param self    Reference to the current work queue object.
    # @param host    The hostname the host running the workers.
    # @param timeout How long this block entry lasts (in seconds). If less than 1, block indefinitely.
    def block_host_with_timeout(self, host, timeout):
        return work_queue_block_host_with_timeout(self._work_queue, host, timeout)

    ##
    # See @ref ndcctools.work_queue.WorkQueue.block_host_with_timeout
    def blacklist_with_timeout(self, host, timeout):
        return self.block_host_with_timeout(host, timeout)

    ##
    # Unblock given host, of all hosts if host not given
    #
    # @param self   Reference to the current work queue object.
    # @param host   The of the hostname the host.
    def unblock_host(self, host=None):
        if host is None:
            return work_queue_unblock_all(self._work_queue)
        return work_queue_unblock_host(self._work_queue, host)

    ##
    # See @ref ndcctools.work_queue.WorkQueue.unblock_host
    def blacklist_clear(self, host=None):
        return self.unblock_host(host)

    ##
    # Delete file from workers's caches.
    #
    # @param self   Reference to the current work queue object.
    # @param local_name   Name of the file as seen by the manager.
    def invalidate_cache_file(self, local_name):
        if local_name:
            local_name = str(local_name)
        return work_queue_invalidate_cached_file(self._work_queue, local_name, WORK_QUEUE_FILE)

    ##
    # Change keepalive interval for a given queue.
    #
    # @param self     Reference to the current work queue object.
    # @param interval Minimum number of seconds to wait before sending new keepalive
    #                 checks to workers.
    def specify_keepalive_interval(self, interval):
        return work_queue_specify_keepalive_interval(self._work_queue, interval)

    ##
    # Change keepalive timeout for a given queue.
    #
    # @param self     Reference to the current work queue object.
    # @param timeout  Minimum number of seconds to wait for a keepalive response
    #                 from worker before marking it as dead.
    def specify_keepalive_timeout(self, timeout):
        return work_queue_specify_keepalive_timeout(self._work_queue, timeout)

    ##
    # Turn on manager capacity measurements.
    #
    # @param self     Reference to the current work queue object.
    #
    def estimate_capacity(self):
        return work_queue_specify_estimate_capacity_on(self._work_queue, 1)

    ##
    # Tune advanced parameters for work queue.
    #
    # @param self  Reference to the current work queue object.
    # @param name  The name fo the parameter to tune. Can be one of following:
    # - "resource-submit-multiplier" Treat each worker as having ({cores,memory,gpus} * multiplier) when submitting tasks. This allows for tasks to wait at a worker rather than the manager. (default = 1.0)
    # - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=10)
    # - "foreman-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)
    # - "transfer-outlier-factor" Transfer that are this many times slower than the average will be aborted.  (default=10x)
    # - "default-transfer-rate" The assumed network bandwidth used until sufficient data has been collected.  (1MB/s)
    # - "fast-abort-multiplier" Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)
    # - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
    # - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
    # - "short-timeout" Set the minimum timeout when sending a brief message to a single worker. (default=5s)
    # - "long-timeout" Set the minimum timeout when sending a brief message to a foreman. (default=1h)
    # - "category-steady-n-tasks" Set the number of tasks considered when computing category buckets.
    # - "hungry-minimum" Mimimum number of tasks to consider queue not hungry. (default=10)
    # - "wait-for-workers" Mimimum number of workers to connect before starting dispatching tasks. (default=0)
    # - "attempt-schedule-depth" The amount of tasks to attempt scheduling on each pass of send_one_task in the main loop. (default=100)
    # - "wait_retrieve_many" Parameter to alter how work_queue_wait works. If set to 0, work_queue_wait breaks out of the while loop whenever a task changes to WORK_QUEUE_TASK_DONE (wait_retrieve_one mode). If set to 1, work_queue_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
    # - "monitor-interval" Parameter to change how frequently the resource monitor records resource consumption of a task in a times series, if this feature is enabled. See @ref enable_monitoring_full.
    # @param value The value to set the parameter to.
    # @return 0 on succes, -1 on failure.
    #
    def tune(self, name, value):
        return work_queue_tune(self._work_queue, name, value)

    ##
    # Submit a task to the queue.
    #
    # It is safe to re-submit a task returned by @ref ndcctools.work_queue.WorkQueue.wait.
    #
    # @param self   Reference to the current work queue object.
    # @param task   A task description created from @ref ndcctools.work_queue.Task.
    def submit(self, task):
        if isinstance(task, RemoteTask):
            task.specify_buffer(json.dumps(task._event), "infile")
        taskid = work_queue_submit(self._work_queue, task._task)
        self._task_table[taskid] = task
        task._manager = self
        return taskid

    ##
    # Wait for tasks to complete.
    #
    # This call will block until the timeout has elapsed
    #
    # @param self       Reference to the current work queue object.
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.  Use an integer to set the timeout or the constant @ref
    #                   WORK_QUEUE_WAITFORTASK to block until a task has completed.
    def wait(self, timeout=WORK_QUEUE_WAITFORTASK):
        return self.wait_for_tag(None, timeout)

    ##
    # Similar to @ref ndcctools.work_queue.WorkQueue.wait, but guarantees that the returned task has the
    # specified tag.
    #
    # This call will block until the timeout has elapsed.
    #
    # @param self       Reference to the current work queue object.
    # @param tag        Desired tag. If None, then it is equivalent to self.wait(timeout)
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.
    def wait_for_tag(self, tag, timeout=WORK_QUEUE_WAITFORTASK):
        self._update_status_display()
        task_pointer = work_queue_wait_for_tag(self._work_queue, tag, timeout)
        if task_pointer:
            if self.empty():
                # if last task in queue, update display
                self._update_status_display(force=True)
            task = self._task_table[int(task_pointer.taskid)]
            del self._task_table[task_pointer.taskid]
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
    # Maps a function to elements in a sequence using work_queue
    #
    # Similar to regular map function in python
    #
    # @param self       Reference to the current work queue object.
    # @param fn         The function that will be called on each element
    # @param seq        The sequence that will call the function
    # @param chunksize The number of elements to process at once

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

            p_task.specify_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i

        n = 0
        for i in range(size + 1):
            while not self.empty() and n < size:
                t = self.wait_for_tag(str(i), 1)
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
    # @param self     Reference to the current work queue object.
    # @param fn       The function that will be called on each element
    # @param seq1     The first seq that will be used to generate pairs
    # @param seq2     The second seq that will be used to generate pairs
    # @param chunksize The number of pairs to process at once
    # @param env      Poncho or conda environment tarball filename

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
                    p_task.specify_environment(env)

                p_task.specify_tag(str(num_task))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num = 0
                num_task += 1
                task.clear()

            task.append(item)
            num += 1

        if len(task) > 0:
            p_task = PythonTask(fpairs, fn, task)
            p_task.specify_tag(str(num_task))
            self.submit(p_task)
            tasks[p_task.id] = num_task
            num_task += 1

        n = 0
        for i in range(num_task):
            while not self.empty() and n < num_task:
                t = self.wait_for_tag(str(i), 10)

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
    # @param self       Reference to the current work queue object.
    # @param fn         The function that will be called on each element
    # @param seq        The seq that will be reduced
    # @param chunksize The number of elements per Task (for tree reduc, must be greater than 1)

    def tree_reduce(self, fn, seq, chunksize=2):
        tasks = {}

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

                p_task.specify_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = i

            n = 0
            for i in range(size + 1):
                while not self.empty() and n < size:
                    t = self.wait_for_tag(str(i), 10)

                    if t:
                        results[tasks[t.id]] = t.output
                        n += 1
                        break

            seq = results

        return seq[0]

    ##
    # Maps a function to elements in a sequence using work_queue remote task
    #
    # Similar to regular map function in python, but creates a task to execute each function on a worker running a coprocess
    #
    # @param self       Reference to the current work queue object.
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
            p_task = RemoteTask(fn, event, coprocess)

            p_task.specify_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i

        n = 0
        for i in range(size + 1):
            while not self.empty() and n < size:
                t = self.wait_for_tag(str(i), 1)
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
    # @param self     Reference to the current work queue object.
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
                p_task = RemoteTask(fn, event, coprocess)
                p_task.specify_tag(str(num_task))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num = 0
                num_task += 1
                task.clear()

            task.append(item)
            num += 1

        if len(task) > 0:
            event = json.dumps({name: task})
            p_task = RemoteTask(fn, event, coprocess)
            p_task.specify_tag(str(num_task))
            self.submit(p_task)
            tasks[p_task.id] = num_task
            num_task += 1

        n = 0
        for i in range(num_task):
            while not self.empty() and n < num_task:
                t = self.wait_for_tag(str(i), 10)
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
    # @param self       Reference to the current work queue object.
    # @param fn         The function that will be called on each element. Exists on the coprocess
    # @param seq        The seq that will be reduced
    # @param coprocess  The name of the coprocess that contains the function fn.
    # @param name       This defines the key in the event json that wraps the data sent to the coprocess.
    # @param chunksize The number of elements per Task (for tree reduc, must be greater than 1)
    def remote_tree_reduce(self, fn, seq, coprocess, name, chunksize=2):
        tasks = {}

        while len(seq) > 1:
            size = math.ceil(len(seq) / chunksize)
            results = [None] * size

            for i in range(size):
                start = i * chunksize
                end = min(len(seq), start + chunksize)

                event = json.dumps({name: seq[start:end]})
                p_task = RemoteTask(fn, event, coprocess)

                p_task.specify_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = i

            n = 0
            for i in range(size + 1):
                while not self.empty() and n < size:
                    t = self.wait_for_tag(str(i), 10)

                    if t:
                        results[tasks[t.id]] = json.loads(t.output)["Result"]
                        n += 1
                        break

            seq = results

        return seq[0]


##
# \class RemoteTask
#
# Python RemoteTask object
#
# This class is used to create a task that will execute on a worker running a coprocess
class RemoteTask(Task):
    ##
    # Create a new remote task specification.
    #
    # @param self       Reference to the current remote task object.
    # @param fn         The name of the function to be executed on the coprocess
    # @param coprocess  The name of the coprocess which has the function you wish to execute.
    #                   The coprocess should have a name() method that returns this
    # @param args       positional arguments used in function to be executed by task. Can be mixed with kwargs
    # @param kwargs	    keyword arguments used in function to be executed by task.
    def __init__(self, fn, coprocess, *args, **kwargs):
        Task.__init__(self, fn)
        self._event = {}
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args
        Task.specify_coprocess(self, coprocess)

    ##
    # Specify function arguments. Accepts arrays and dictionarys. This overrides any arguments passed during task creation
    # @param self             Reference to the current remote task object
    # @param args             An array of positional args to be passed to the function
    # @param kwargs           A dictionary of keyword arguments to be passed to the function
    def specify_fn_args(self, args=[], kwargs={}):
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args

    ##
    # Specify how the remote task should execute
    # @param self                     Reference to the current remote task object
    # @param remote_task_exec_method  Can be one of "fork", "direct", or "thread". Fork creates a child process to execute the function, direct has the worker directly call the function, and thread spawns a thread to execute the function
    def specify_exec_method(self, remote_task_exec_method):
        if remote_task_exec_method not in ["fork", "direct", "thread"]:
            print("Error, work_queue_exec_method must be one of fork, direct, or thread")
        self._event["remote_task_exec_method"] = remote_task_exec_method


##
# \class Factory
# Launch a Work Queue factory.
#
# The command line arguments for `work_queue_factory` can be set for a
# factory object (with dashes replaced with underscores). Creating a factory
# object does not immediately launch it, so this is a good time to configure
# the resources, number of workers, etc. Factory objects function as Python
# context managers, so to indicate that a set of commands should be run with
# a factory running, wrap them in a `with` statement. The factory will be
# cleaned up automatically at the end of the block. You can also make
# config changes to the factory while it is running. As an example,
#
#     # normal WQ setup stuff
#     workers = work_queue.Factory("sge", "myproject")
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
        self._opts["worker-binary"] = self._find_exe(worker_binary, "work_queue_worker")
        self._factory_binary = self._find_exe(factory_binary, "work_queue_factory")
        self._opts["scratch-dir"] = None

        def free():
            if self._factory_proc is not None:
                self.stop()
            if self._scratch_safe_to_delete and self.scratch_dir and os.path.exists(self.scratch_dir):
                shutil.rmtree(self.scratch_dir)
        self._finalizer = weakref.finalize(self, free)

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
            out = shutil.which(default)
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
            candidate = os.path.join(candidate, f"wq-factory-{os.getuid()}")
            if not os.path.exists(candidate):
                os.makedirs(candidate)
            self.scratch_dir = candidate

        # specialize scratch_dir for this run
        self.scratch_dir = tempfile.mkdtemp(prefix="wq-factory-", dir=self.scratch_dir)
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
                raise RuntimeError("Could not execute work_queue_factory. Exited with status: {}\n{}".format(str(status), error_log))
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

    def _write_config(self):
        if self._config_file is None:
            return

        opts_subset = dict([(opt, self._opts[opt]) for opt in self._opts if opt in Factory._config_file_options])
        with open(self._config_file, "w") as f:
            json.dump(opts_subset, f, indent=4)

    def specify_environment(self, env):
        self._env_file = env

    specify_package = specify_environment

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
