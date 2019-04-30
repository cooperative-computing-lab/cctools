## @package WorkQueuePython
#
# Python Work Queue bindings.
#
# The objects and methods provided by this package correspond to the native
# C API in @ref work_queue.h.
#
# The SWIG-based Python bindings provide a higher-level interface that
# revolves around the following objects:
#
# - @ref work_queue::WorkQueue
# - @ref work_queue::Task

import copy
import os

def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)

def specify_debug_log(logfile):
    set_debug_flag('all')
    cctools_debug_config_file_size(0)
    cctools_debug_config_file(logfile)

def specify_port_range(low_port, high_port):
    if low_port >= high_port:
        raise TypeError('low_port {} should be smaller than high_port {}'.format(low_port, high_port))

    os.environ['TCP_LOW_PORT']  = str(low_port)
    os.environ['TCP_HIGH_PORT'] = str(high_port)

cctools_debug_config('work_queue_python')

##
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

        try:
            self._task = work_queue_task_create(command)
            if not self._task:
                raise
        except:
            raise Exception('Unable to create internal Task structure')

    def __del__(self):
        if self._task:
            work_queue_task_delete(self._task)

    @staticmethod
    def _determine_file_flags(flags, cache):
        # if flags is defined, use its value. Otherwise do not cache only if
        # asked explicitely.

        if flags is None:
            flags = WORK_QUEUE_NOCACHE;

        if cache is not None:
            if cache:
                flags = flags | WORK_QUEUE_CACHE;
            else:
                flags = flags & ~(WORK_QUEUE_CACHE);

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
    # Indicate that the task would be optimally run on a given host.
    #
    # @param self       Reference to the current task object.
    # @param hostname   The hostname to which this task would optimally be sent.
    def specify_preferred_host(self, hostname):
        return work_queue_task_specify_preferred_host(self._task, hostname)

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
    # @param cache          Legacy parameter for setting file caching attribute. (True/False, deprecated, use the flags parameter.)
    #
    # For example:
    # @code
    # # The following are equivalent
    # >>> task.specify_file("/etc/hosts", type=WORK_QUEUE_INPUT, flags=WORK_QUEUE_CACHE)
    # >>> task.specify_file("/etc/hosts", "hosts", type=WORK_QUEUE_INPUT)
    # @endcode
    def specify_file(self, local_name, remote_name=None, type=None, flags=None, cache=None):
        if remote_name is None:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_file(self._task, local_name, remote_name, type, flags)

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
    # @param cache          Legacy parameter for setting file caching attribute. (True/False, deprecated, use the flags parameter.)
    def specify_file_piece(self, local_name, remote_name=None, start_byte=0, end_byte=0, type=None, flags=None, cache=None):
        if remote_name is None:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_file_piece(self._task, local_name, remote_name, start_byte, end_byte, type, flags)

    ##
    # Add a input file to the task.
    #
    # This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_INPUT.
    def specify_input_file(self, local_name, remote_name=None, flags=None, cache=None):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_INPUT, flags, cache)

    ##
    # Add a output file to the task.
    #
    # This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_OUTPUT.
    def specify_output_file(self, local_name, remote_name=None, flags=None, cache=None):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_OUTPUT, flags, cache)

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
    # @param cache          Legacy parameter for setting file caching attribute. (True/False, deprecated, use the flags parameter.)
    # @return 1 if the task directory is successfully specified, 0 if either of @a local_name, or @a remote_name is null or @a remote_name is an absolute path.
    def specify_directory(self, local_name, remote_name=None, type=None, flags=None, recursive=False, cache=None):
        if remote_name is None:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_directory(self._task, local_name, remote_name, type, flags, recursive)

    ##
    # Add an input bufer to the task.
    #
    # @param self           Reference to the current task object.
    # @param buffer         The contents of the buffer to pass as input.
    # @param remote_name    The name of the remote file to create.
    # @param flags          May take the same values as @ref specify_file.
    # @param cache          Legacy parameter for setting buffer caching attribute. (True/False, deprecated, use the flags parameter.)
    def specify_buffer(self, buffer, remote_name, flags=None, cache=None):
        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_buffer(self._task, buffer, len(buffer), remote_name, flags)

    # When monitoring, indicates a json-encoded file that instructs the monitor
    # to take a snapshot of the task resources. Snapshots appear in the JSON
    # summary file of the task, under the key "snapshots". Snapshots are taken
    # on events on files described in the monitor_snapshot_file. The
    # monitor_snapshot_file is a json encoded file with the following format:
    #
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
    def specify_max_retries( self, max_retries ):
        return work_queue_task_specify_max_retries(self._task,max_retries)

    ##
    # Indicate the number of cores required by this task.
    def specify_cores( self, cores ):
        return work_queue_task_specify_cores(self._task,cores)

    ##
    # Indicate the memory (in MB) required by this task.
    def specify_memory( self, memory ):
        return work_queue_task_specify_memory(self._task,memory)

    ##
    # Indicate the disk space (in MB) required by this task.
    def specify_disk( self, disk ):
        return work_queue_task_specify_disk(self._task,disk)

    ##
    # Indicate the the priority of this task (larger means better priority, default is 0).
    def specify_priority( self, priority ):
        return work_queue_task_specify_priority(self._task,priority)

    # Indicate the maximum end time (absolute, in microseconds from the Epoch) of this task.
    # This is useful, for example, when the task uses certificates that expire.
    # If less than 1, or not specified, no limit is imposed.
    def specify_end_time( self, useconds ):
        return work_queue_task_specify_end_time(self._task,useconds)

    # Indicate the maximum running time for a task in a worker (relative to
    # when the task starts to run).  If less than 1, or not specified, no limit
    # is imposed.
    def specify_running_time( self, useconds ):
        return work_queue_task_specify_running_time(self._task,useconds)

    ##
    # Set this environment variable before running the task.
    # If value is None, then variable is unset.
    def specify_environment_variable( self, name, value = None ):
        return work_queue_task_specify_enviroment_variable(self._task,name,value)

    ##
    # Set a name for the resource summary output directory from the monitor.
    def specify_monitor_output( self, directory ):
        return work_queue_task_specify_monitor_output(self._task,directory)

    ##
    # Get the user-defined logical name for the task.
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.tag
    # @endcode
    @property
    def tag(self):
        return self._task.tag

    ##
    # Get the category name for the task.
    #
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.category
    # @endcode
    @property
    def category(self):
        return self._task.category

    ##
    # Get the shell command executed by the task.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.command
    # @endcode
    @property
    def command(self):
        return self._task.command_line

    ##
    # Get the priority of the task.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.priority
    # @endcode
    @property
    def priority(self):
        return self._task.priority

    ##
    # Get the algorithm for choosing worker to run the task.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.algorithm
    # @endcode
    @property
    def algorithm(self):
        return self._task.worker_selection_algorithm

    ##
    # Get the standard output of the task. Must be called only after the task
	# completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.output
    # @endcode
    @property
    def output(self):
        return self._task.output

    ##
    # Get the task id number. Must be called only after the task was submitted.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.id
    # @endcode
    @property
    def id(self):
        return self._task.taskid

    ##
    # Get the exit code of the command executed by the task. Must be called only
	# after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.return_status
    # @endcode
    @property
    def return_status(self):
        return self._task.return_status

    ##
    # Get the result of the task, such as successful, missing file, etc.
    # See @ref work_queue_result_t for possible values.  Must be called only
    # after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.result
    # @endcode
    @property
    def result(self):
        return self._task.result

    ##
    # Get the number of times the task has been resubmitted internally.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_submissions
    # @endcode
    @property
    def total_submissions(self):
        return self._task.total_submissions

    ##
    # Get the number of times the task has been failed given resource exhaustion.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.exhausted_attempts
    # @endcode
    @property
    def exhausted_attempts(self):
        return self._task.exhausted_attempts

    ##
    # Get the address and port of the host on which the task ran.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.host
    # @endcode
    @property
    def host(self):
        return self._task.host

    ##
    # Get the name of the host on which the task ran.
	# Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.hostname
    # @endcode
    @property
    def hostname(self):
        return self._task.hostname

    ##
    # Get the time at which this task was submitted.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.submit_time
    # @endcode
    @property
    def submit_time(self):
        return self._task.time_task_submit

    ##
    # Get the time at which this task was finished.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.finish_time
    # @endcode
    @property
    def finish_time(self):
        return self._task.time_task_finish

    ##
    # Get the total time the task executed and failed given resource exhaustion.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_cmd_exhausted_execute_time
    # @endcode
    @property
    def total_cmd_exhausted_execute_time(self):
        return self._task.total_cmd_exhausted_execute_time

    ##
    # Get the time spent in upper-level application (outside of work_queue_wait).
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.app_delay
    # @endcode
    @property
    def app_delay(self):
        return self._task.time_app_delay

    ##
    # Get the time at which the task started to transfer input files.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.send_input_start
    # @endcode
    @property
    def send_input_start(self):
        return self._task.time_send_input_start

    ##
    # Get the time at which the task finished transferring input files.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.send_input_finish
    # @endcode
    @property
    def send_input_finish(self):
        return self._task.time_send_input_finish

    ##
    # The time at which the task began.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.execute_cmd_start
    # @endcode
    @property
    def execute_cmd_start(self):
        return self._task.time_execute_cmd_start

    ##
    # Get the time at which the task finished (discovered by the master).
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.execute_cmd_finish
    # @endcode
    @property
    def execute_cmd_finish(self):
        return self._task.time_execute_cmd_finish

    ##
	# Get the time at which the task started to transfer output files.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.receive_output_start
    # @endcode
    @property
    def receive_output_start(self):
        return self._task.time_receive_output_start

    ##
    # Get the time at which the task finished transferring output files.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.receive_output_finish
    # @endcode
    @property
    def receive_output_finish(self):
        return self._task.time_receive_output_finish

    ##
    # Get the number of bytes received since task started receiving input data.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_bytes_received
    # @endcode
    @property
    def total_bytes_received(self):
        return self._task.total_bytes_received

    ##
    # Get the number of bytes sent since task started sending input data.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_bytes_sent
    # @endcode
    @property
    def total_bytes_sent(self):
        return self._task.total_bytes_sent

    ##
    # Get the number of bytes transferred since task started transferring input data.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_bytes_transferred
    # @endcode
    @property
    def total_bytes_transferred(self):
        return self._task.total_bytes_transferred

    ##
    # Get the time comsumed in microseconds for transferring total_bytes_transferred.
	# Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_transfer_time
    # @endcode
    @property
    def total_transfer_time(self):
        return self._task.total_transfer_time

    ##
    # Time spent in microseconds for executing the command until completion on a single worker.
    # Must be called only after the task completes execution.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.cmd_execution_time
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
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print t.total_cmd_execution_time
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
    # >>> print t.resources_measured.memory
    # @endcode
    @property
    def resources_measured(self):
        if not self._task.resources_measured:
            return None

        return self._task.resources_measured

    ##
    # Get the resources the task exceeded. For valid field see @resources_measured.
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
    # @resources_measured.
    #
    @property
    def resources_requested(self):
        if not self._task.resources_requested:
            return None
        return self._task.resources_requested

    ##
    # Get the resources allocated to the task in its latest attempt. For valid
    # fields see @resources_measured.
    #
    @property
    def resources_allocated(self):
        if not self._task.resources_allocated:
            return None
        return self._task.resources_allocated


##
# Python Work Queue object
#
# This class uses a dictionary to map between the task pointer objects and the
# @ref work_queue::Task.
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
    #
    # @see work_queue_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=WORK_QUEUE_DEFAULT_PORT, name=None, shutdown=False, stats_log=None, transactions_log=None, debug_log=None):
        self._shutdown   = shutdown
        self._work_queue = None
        self._stats      = None
        self._stats_hierarchy = None
        self._task_table = {}

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
            raise ValueError('port should be a single integer, or a sequence of two integers')

        try:
            if debug_log:
                specify_debug_log(debug_log)
            self._stats      = work_queue_stats()
            self._stats_hierarchy = work_queue_stats()
            self._work_queue = work_queue_create(port)
            if not self._work_queue:
                raise Exception('Could not create work_queue on port %d' % port)

            if stats_log:
                self.specify_log(stats_log)

            if transactions_log:
                self.specify_transactions_log(transactions_log)

            if name:
                work_queue_specify_name(self._work_queue, name)
        except Exception, e:
            raise Exception('Unable to create internal Work Queue structure: %s' % e)

    def __free_queue(self):
        if self._work_queue:
            if self._shutdown:
                self.shutdown_workers(0)
            work_queue_delete(self._work_queue)

    def __exit__(self):
        self.__free_queue()

    def __del__(self):
        self.__free_queue()

    ##
    # Get the project name of the queue.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print q.name
    # @endcode
    @property
    def name(self):
        return work_queue_name(self._work_queue)

    ##
    # Get the listening port of the queue.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print q.port
    # @endcode
    @property
    def port(self):
        return work_queue_port(self._work_queue)

    ##
    # Get queue statistics.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print q.stats
    # @endcode
    # The fields in @ref stats can also be individually accessed through this call. For example:
    # @code
    # >>> print q.stats.workers_busy
    # @endcode
    @property
    def stats(self):
        work_queue_get_stats(self._work_queue, self._stats)
        return self._stats

    ##
    # Get worker hierarchy statistics.
    # @a Note: This is defined using property decorator. So it must be called without parentheses
    # (). For example:
    # @code
    # >>> print q.stats_hierarchy
    # @endcode
    # The fields in @ref stats_hierarchy can also be individually accessed through this call. For example:
    # @code
    # >>> print q.stats_hierarchy.workers_busy
    # @endcode
    @property
    def stats_hierarchy(self):
        work_queue_get_stats_hierarchy(self._work_queue, self._stats_hierarchy)
        return self._stats_hierarchy

    ##
    # Get the task statistics for the given category.
    #
    # @param self 	Reference to the current work queue object.
    # @param category   A category name.
    # For example:
    # @code
    # s = q.stats_category("my_category")
    # >>> print s
    # @endcode
    # The fields in @ref work_queue_stats can also be individually accessed through this call. For example:
    # @code
    # >>> print s.tasks_waiting
    # @endcode
    def stats_category(self, category):
        stats = work_queue_stats()
        work_queue_get_stats_category(self._work_queue, category, stats)
        return stats

    ##
    # Turn on or off first-allocation labeling for a given category. By
    # default, only cores, memory, and disk are labeled. Turn on/off other
    # specific resources with @ref specify_category_autolabel_resource.
    # NOTE: autolabeling is only meaningfull when task monitoring is enabled
    # (@ref enable_monitoring). When monitoring is enabled and a task exhausts
    # resources in a worker, mode dictates how work queue handles the
    # exhaustion:
    # @param self Reference to the current work queue object.
    # @param category A category name. If None, sets the mode by default for
    # newly created categories.
    # @param mode One of @ref category_mode_t:
    #                  - WORK_QUEUE_ALLOCATION_MODE_FIXED Task fails (default).
    #                  - WORK_QUEUE_ALLOCATION_MODE_MAX If maximum values are
    #                  specified for cores, memory, or disk (e.g. via @ref
    #                  specify_max_category_resources or @ref specify_memory),
    #                  and one of those resources is exceeded, the task fails.
    #                  Otherwise it is retried until a large enough worker
    #                  connects to the master, using the maximum values
    #                  specified, and the maximum values so far seen for
    #                  resources not specified. Use @ref specify_max_retries to
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
    # specify_category_mode.
    # @param q A work queue object.
    # @param category A category name.
    # @param resource A resource name.
    # @param autolabel True/False for on/off.
    # @returns 1 if resource is valid, 0 otherwise.
    def specify_category_autolabel_resource(self, category, resource, autolabel):
        return work_queue_enable_category_resource(self._work_queue, category, category, resource, autolabel)

    ##
    # Get current task state. See @ref work_queue_task_state_t for possible values.
    # @code
    # >>> print q.task_state(taskid)
    # @endcode
    def task_state(self, taskid):
        return work_queue_task_state(self._work_queue, taskid)

    ## Enables resource monitoring of tasks in the queue, and writes a summary
    #  per task to the directory given. Additionally, all summaries are
    #  consolidate into the file all_summaries-PID.log
    #
    #  Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self 	Reference to the current work queue object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring(self, dirname = None, watchdog = True):
        return work_queue_enable_monitoring(self._work_queue, dirname, watchdog)

    ## As @ref enable_monitoring, but it also generates a time series and a debug file.
    #  WARNING: Such files may reach gigabyte sizes for long running tasks.
    #
    #  Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self 	Reference to the current work queue object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring_full(self, dirname = None, watchdog = True):
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
    # @param multiplier The multiplier of the average task time at which point to abort; if zero, deacticate for the category, negative (the default), use the one for the "default" category (see @ref fast_abort)
    def activate_fast_abort_category(self, name, multiplier):
        return work_queue_activate_fast_abort_category(self._work_queue, name, multiplier)

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
    # @param order  	One of the following algorithms to use in dispatching
	# 					submitted tasks to workers:
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
    # 'by_ip' uses IP address (standard behavior), or 'by_hostname' to use the
    # hostname at the master.
    #
    # @param self   Reference to the current work queue object.
    # @param preferred_connection An string to indicate using 'by_ip' or a 'by_hostname'.
    def specify_master_preferred_connection(self, mode):
        return work_queue_master_preferred_connection(self._work_queue, mode)

    ##
    # Set the minimum taskid of future submitted tasks.
    #
    # Further submitted tasks are guaranteed to have a taskid larger or equal
    # to minid.  This function is useful to make taskids consistent in a
    # workflow that consists of sequential masters. (Note: This function is
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
    # @param priority   An integer that presents the priorty of this work queue master. The higher the value, the higher the priority.
    def specify_priority(self, priority):
        return work_queue_specify_priority(self._work_queue, priority)

    ## Specify the number of tasks not yet submitted to the queue.
    # It is used by work_queue_factory to determine the number of workers to launch.
    # If not specified, it defaults to 0.
    # work_queue_factory considers the number of tasks as:
    # num tasks left + num tasks running + num tasks read.
    # @param q A work queue object.
    # @param ntasks Number of tasks yet to be submitted.
    def specify_num_tasks_left(self, ntasks):
        return work_queue_specify_num_tasks_left(self._work_queue, ntasks)

    ##
    # Specify the master mode for the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param mode   This may be one of the following values: @ref WORK_QUEUE_MASTER_MODE_STANDALONE or @ref WORK_QUEUE_MASTER_MODE_CATALOG.
    def specify_master_mode(self, mode):
        return work_queue_specify_master_mode(self._work_queue, mode)

    ##
    # Specify the catalog server the master should report to.
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
    # @param rm        Dictionary indicating maximum values. See @resources_measured for possible fields.
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
            setattr(rm, k, rmd[k])
        return work_queue_specify_max_resources(self._work_queue, rm)

    ##
    # Specifies the maximum resources allowed for the given category.
    #
    # @param self      Reference to the current work queue object.
    # @param category  Name of the category.
    # @param rm        Dictionary indicating maximum values. See @resources_measured for possible fields.
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
            setattr(rm, k, rmd[k])
        return work_queue_specify_category_max_resources(self._work_queue, category, rm)

    ##
    # Specifies the first-allocation guess for the given category
    #
    # @param self      Reference to the current work queue object.
    # @param category  Name of the category.
    # @param rm        Dictionary indicating maximum values. See @resources_measured for possible fields.
    # For example:
    # @code
    # >>> # A maximum of 4 cores may be used by a task in the category:
    # >>> q.specify_max_category_resources("my_category", {'cores': 4})
    # >>> # A maximum of 8 cores, 1GB of memory, and 10GB may be used by a task:
    # >>> q.specify_max_category_resources("my_category", {'cores': 8, 'memory':  1024, 'disk': 10240})
    # @endcode

    def specify_category_first_allocation_guess(self, category, rmd):
        rm = rmsummary_create(-1)
        for k in rmd:
            old_value = getattr(rm, k) # to raise an exception for unknown keys
            setattr(rm, k, rmd[k])
        return work_queue_specify_category_first_allocation_guess(self._work_queue, category, rm)

    ##
    # Initialize first value of categories
    #
    # @param self     Reference to the current work queue object.
    # @param rm       Dictionary indicating maximum values. See @resources_measured for possible fields.
    # @param filename JSON file with resource summaries.

    def initialize_categories(filename, rm):
        return work_queue_initialize_categories(self._work_queue, rm, filename)

    ##
    # Cancel task identified by its taskid and remove from the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param id     The taskid returned from @ref submit.
    def cancel_by_taskid(self, id):
        return work_queue_cancel_by_taskid(self._work_queue, id)

    ##
    # Cancel task identified by its tag and remove from the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param tag    The tag assigned to task using @ref work_queue_task_specify_tag.
    def cancel_by_tasktag(self, tag):
        return work_queue_cancel_by_tasktag(self._work_queue, tag)

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
    # Blacklist workers running on host.
    #
    # @param self   Reference to the current work queue object.
    # @param host   The hostname the host running the workers.
    def blacklist(self, host):
        return work_queue_blacklist_add(self._work_queue, host)

    ##
    # Blacklist workers running on host for the duration of the given timeout.
    #
    # @param self    Reference to the current work queue object.
    # @param host    The hostname the host running the workers.
    # @param timeout How long this blacklist entry lasts (in seconds). If less than 1, blacklist indefinitely.
    def blacklist_with_timeout(self, host, timeout):
        return work_queue_blacklist_add_with_timeout(self._work_queue, host, timeout)

    ##
    # Remove host from blacklist. Clear all blacklist if host not provided.
    #
    # @param self   Reference to the current work queue object.
    # @param host   The of the hostname the host.
    def blacklist_clear(self, host=None):
        if host is None:
            return work_queue_blacklist_clear(self._work_queue)
        else:
            return work_queue_blacklist_remove(self._work_queue, host)

    ##
    # Delete file from workers's caches.
    #
    # @param self   Reference to the current work queue object.
    # @param local_name   Name of the file as seen by the master.
    def invalidate_cache_file(self, local_name):
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
    # Turn on master capacity measurements.
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
    #              - "asynchrony-multiplier" Treat each worker as having (actual_cores * multiplier) total cores. (default = 1.0)
    #              - "asynchrony-modifier" Treat each worker as having an additional "modifier" cores. (default=0)
    #              - "min-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a worker. (default=300)
    #              - "foreman-transfer-timeout" Set the minimum number of seconds to wait for files to be transferred to or from a foreman. (default=3600)
    #              - "fast-abort-multiplier" Set the multiplier of the average task time at which point to abort; if negative or zero fast_abort is deactivated. (default=0)
    #              - "keepalive-interval" Set the minimum number of seconds to wait before sending new keepalive checks to workers. (default=300)
    #              - "keepalive-timeout" Set the minimum number of seconds to wait for a keepalive response from worker before marking it as dead. (default=30)
    # @param value The value to set the parameter to.
    # @return 0 on succes, -1 on failure.
    #
    def tune(self, name, value):
        return work_queue_tune(self._work_queue, name, value)

    ##
    # Submit a task to the queue.
    #
    # It is safe to re-submit a task returned by @ref wait.
    #
    # @param self   Reference to the current work queue object.
    # @param task   A task description created from @ref work_queue::Task.
    def submit(self, task):
        taskid = work_queue_submit(self._work_queue, task._task)
        self._task_table[taskid] = task
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
        task_pointer = work_queue_wait(self._work_queue, timeout)
        if task_pointer:
            task = self._task_table[int(task_pointer.taskid)]
            del(self._task_table[task_pointer.taskid])
            return task
        return None

def rmsummary_snapshots(self):
    if self.snapshots_count < 1:
        return None

    snapshots = []
    for i in range(0, self.snapshots_count):
        snapshot = rmsummary_get_snapshot(self, i);
        snapshots.append(snapshot)
    return snapshots

rmsummary.snapshots = property(rmsummary_snapshots)
