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

import os

def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)

cctools_debug_config('work_queue_python')

##
# Python Task object
#
# This class is used to create a task specification.
class Task(_object):

    ##
    # Create a new task specification.
    #
    # @param self       Reference to the current task object.
    # @param command    The shell command line to be exected by the task.
    def __init__(self, command):
        self._task = work_queue_task_create(command)

    def __del__(self):
        work_queue_task_delete(self._task)

    @staticmethod
    def _determine_file_flags(flags, cache):
        if flags is None:
            flags = WORK_QUEUE_CACHE

        if cache:
            flags |= WORK_QUEUE_CACHE
        else:
            flags &= ~WORK_QUEUE_CACHE

        return flags

    @property
    def algorithm(self):
        return self._task.worker_selection_algorithm

    @property
    def command(self):
        return self._task.command_line

    @property
    def tag(self):
        return self._task.tag

    @property
    def output(self):
        return self._task.output

    @property
    def id(self):
        return self._task.taskid

    @property
    def return_status(self):
        return self._task.return_status

    @property
    def result(self):
        return self._task.result
	
    @property
    def hostname(self):
        return self._task.hostname

    @property
    def host(self):
        return self._task.host

    @property
    def submit_time(self):
        return self._task.time_task_submit

    @property
    def finish_time(self):
        return self._task.time_task_finish

    @property
    def app_delay(self):
        return self._task.time_app_delay

    @property
    def send_input_start(self):
        return self._task.time_send_input_start
    
    @property
    def send_input_finish(self):
        return self._task.time_send_input_finish
    
    @property
    def execute_cmd_start(self):
        return self._task.time_execute_cmd_start
    
    @property
    def execute_cmd_finish(self):
        return self._task.time_execute_cmd_finish
    
    @property
    def receive_output_start(self):
        return self._task.time_receive_output_start
    
    @property
    def receive_output_finish(self):
        return self._task.time_receive_output_finish
    
    @property
    def total_bytes_transferred(self):
        return self._task.total_bytes_transferred
    
    @property
    def total_transfer_time(self):
        return self._task.total_transfer_time
    
    @property
    def cmd_execution_time(self):
        return self._task.cmd_execution_time 

    ##
    # Set the worker selection algorithm for task.
    #
    # @param self       Reference to the current task object.
    # @param algorithm  One of the following algorithms to use in assigning a
    #                   task to a worker:
    #                   - @ref WORK_QUEUE_SCHEDULE_FCFS
    #                   - @ref WORK_QUEUE_SCHEDULE_FILES
    #                   - @ref WORK_QUEUE_SCHEDULE_TIME
    #                   - @ref WORK_QUEUE_SCHEDULE_RAND
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
    # @param flags          May be zero to indicate no special handling, or any of the following or'd together:
    #                       - @ref WORK_QUEUE_NOCACHE
    #                       - @ref WORK_QUEUE_CACHE
    # @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
    #
    # Example:
    # @code
    # # The following are equivalent
    # >>> task.specify_file("/etc/hosts", type=WORK_QUEUE_INPUT, flags=WORK_QUEUE_NOCACHE)
    # >>> task.specify_file("/etc/hosts", "hosts", type=WORK_QUEUE_INPUT, cache=false)
    # @endcode
    def specify_file(self, local_name, remote_name=None, type=None, flags=None, cache=True):
        if remote_name is None:
            remote_name = os.path.basename(local_name)

        if type is None:
            type = WORK_QUEUE_INPUT

        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_file(self._task, local_name, remote_name, type, flags)

    ##
    # Add a input file to the task.
    #
    # This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_INPUT.
    def specify_input_file(self, local_name, remote_name=None, flags=None, cache=True):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_INPUT, flags, cache)

    ##
    # Add a output file to the task.
    #
    # This is just a wrapper for @ref specify_file with type set to @ref WORK_QUEUE_OUTPUT.
    def specify_output_file(self, local_name, remote_name=None, flags=None, cache=True):
        return self.specify_file(local_name, remote_name, WORK_QUEUE_OUTPUT, flags, cache)

    ##
    # Add an input bufer to the task.
    #
    # @param self           Reference to the current task object.
    # @param buffer         The contents of the buffer to pass as input.
    # @param remote_name    The name of the remote file to create.
    # @param flags          May take the same values as @ref specify_file.
    # @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
    def specify_buffer(self, buffer, remote_name, flags=None, cache=True):
        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_buffer(self._task, buffer, len(buffer), remote_name, flags)

    ##
    # Add a file created or handled by an arbitrary command to a task (eg. wget, ftp, chirp_get|put).
    #
    # @param self           Reference to the current task object.
    # @param remote_name    The name of the remote file at the execution site.
    # @param command        The contents of the buffer to pass as input.
    # @param type           Must be one of the following values: @ref WORK_QUEUE_INPUT or @ref WORK_QUEUE_OUTPUT
    # @param flags          May take the same values as @ref specify_file.
    # @param cache          Legacy parameter for setting file caching attribute.  By default this is enabled.
    def specify_file_command(self, remote_name, command, type, flags, cache=True):
        flags = Task._determine_file_flags(flags, cache)
        return work_queue_task_specify_file_command(self._task, remote_name, command, type, flags)


##
# Python Work Queue object
#
# This class uses a dictionary to map between the task pointer objects and the
# @ref work_queue::Task.
class WorkQueue(_object):
    ##
    # Create a new work queue.
    #
    # @param self       Reference to the current work queue object.
    # @param port       The port number to listen on. If zero is specified, then the default is chosen, and if -1 is specified, a random port is chosen.
    # @param name       The project name to use.
    # @param catalog    Whether or not to enable catalog mode.
    # @param exclusive  Whether or not the workers should be exclusive.
    # @param shutdown   Automatically shutdown workers when queue is finished. Disabled by default.
    #
    # @see work_queue_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=WORK_QUEUE_DEFAULT_PORT, name=None, catalog=False, exclusive=True, shutdown=False):
        self._shutdown   = shutdown
        self._work_queue = work_queue_create(port)
        if not self._work_queue:
            raise 

        self._stats      = work_queue_stats()
        self._task_table = {}

        if name:
            work_queue_specify_name(self._work_queue, name)

        work_queue_specify_master_mode(self._work_queue, catalog)

    def __del__(self):
        if self._shutdown:
            self.shutdown_workers(0)
        work_queue_delete(self._work_queue)
    
    @property
    def name(self):
        return work_queue_name(self._work_queue)

    @property
    def port(self):
        return work_queue_port(self._work_queue)

    @property
    def stats(self):
        work_queue_get_stats(self._work_queue, self._stats)
        return self._stats

    ##
    # Turn on or off fast abort functionality for a given queue.
    #
    # @param self       Reference to the current work queue object.
    # @param multiplier The multiplier of the average task time at which point to abort; if negative (the default) fast_abort is deactivated.
    def activate_fast_abort(self, multiplier):
        return work_queue_activate_fast_abort(self._work_queue, multiplier)

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
    #                   task to a worker:
    #                   - @ref WORK_QUEUE_SCHEDULE_FCFS
    #                   - @ref WORK_QUEUE_SCHEDULE_FILES
    #                   - @ref WORK_QUEUE_SCHEDULE_TIME
    #                   - @ref WORK_QUEUE_SCHEDULE_RAND
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
    # Change the project priority for the given queue.
    #
    # @param self       Reference to the current work queue object.
    # @param priority   An integer that presents the priorty of this work queue master. The higher the value, the higher the priority.
    def specify_priority(self, priority):
        return work_queue_specify_priority(self._work_queue, priority)

    ##
    # Specify the master mode for the given queue.
    #
    # @param self   Reference to the current work queue object.
    # @param mode   This may be one of the following values: @ref WORK_QUEUE_MASTER_MODE_STANDALONE or @ref WORK_QUEUE_MASTER_MODE_CATALOG.
    def specify_master_mode(self, mode):
        return work_queue_specify_master_mode(self._work_queue, mode)

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
