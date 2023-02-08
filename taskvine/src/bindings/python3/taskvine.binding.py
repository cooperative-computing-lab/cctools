## @package taskvine
#
# Python API for the TaskVine workflow framework.
#
# TaskVine is a framework for building large scale distributed data intensive
# applications that run on clusters, clouds, grids, and similar distributed systems.
# A TaskVine application consists of a main program that creates a @ref Manager object,
# and then submits @ref Tasks that use @ref Files representing data sources.
# The manager distributes tasks across available workers and returns results to
# the main application.
#
# See the <a href=http://cctools.readthedocs.io/en/latest/taskvine>TaskVine Manual</a> for complete documentation.
#
# - @ref Manager
# - @ref Task / @ref PythonTask / @ref RemoteTask
# - @ref File / @ref FileLocal / @ref FileURL / @ref FileBuffer / @ref FileMiniTask
# - @ref Factory
#
# The objects and methods provided by this package correspond closely
# to the native C API in @ref vine_manager.h.
#

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
import math

def set_port_range(low_port, high_port):
    if low_port > high_port:
        raise TypeError('low_port {} should be smaller than high_port {}'.format(low_port, high_port))

    os.environ['TCP_LOW_PORT'] = str(low_port)
    os.environ['TCP_HIGH_PORT'] = str(high_port)

staging_directory = tempfile.mkdtemp(prefix='vine-py-staging-')
def cleanup_staging_directory():
    try:
        shutil.rmtree(staging_directory)
    except Exception as e:
        sys.stderr.write('could not delete {}: {}\n'.format(staging_directory, e))

atexit.register(cleanup_staging_directory)

##
# \class File
#
# TaskVine File Object
#
# The superclass of all TaskVine file types.

class File(object):
    def __del__(self):
        try:
            if self._file:
                vine_file_delete(self._file)
        except:
            #ignore exceptions, in case task has been already collected
            pass

##
# \class FileLocal
#
# TaskVine File object
#
# A file obtained from the local filesystem.

class FileLocal(File):

    ##
    # Create a local file object.
    #
    # @param self       The current file object.
    # @param path       The path to the local file.
    
    def __init__(self,path):
        path = str(path)
        self._file = vine_file_local(path)

##
# \class FileURL
#
# TaskVine URL object
#
# A file obtained from a remote URL.

class FileURL(File):
    ##
    # Create a remote URL file object.
    #
    # @param self      The current file object.
    # @param url       The url of the file.
        
    def __init__(self,url):
        url = str(url)
        self._file = vine_file_url(url)

##
# \class FileBuffer
#
# TaskVine Buffer object
#
# A file obtained from a buffer in memory.

class FileBuffer(File):
    ##
    # Create a file from a buffer in memory.
    #
    # @param self       The current file object.
    # @param name       The abstract name of the buffer.
    # @param buffer     The contents of the buffer.
        
    def __init__(self,name,buffer):
        name = str(name)
        buffer = str(buffer)
        self._file = vine_file_buffer(name,buffer,len(buffer))

##
# \class FileMiniTask
#
# TaskVine File object
#
# A file obtained from a mini-task.

class FileMiniTask(File):
    ##
    # Create a file by executing a mini-task.
    #
    # @param self       The current file object.
    # @param minitask   The task to execute in order to produce a file.
        
    def __init__(self,minitask):
        self._file = vine_file_mini_task(minitask._task)

##
# \class FileUntar
#
# TaskVine File TAR Unpacker
#
# A wrapper to unpack a file in .tar form.

class FileUntar(File):
    ##
    # Create a file by unpacking a tar file.
    #
    # @param self       The current file object.
    # @param subfile    The file object to un-tar.
        
    def __init__(self,subfile):
        self._file = vine_file_untar(vine_file_clone(subfile._file))

##
# \class FilePoncho
#
# TaskVine File PONCHO Unpacker
#
# A wrapper to unpack a file in poncho package form.

class FileUnponcho(File):
    ##
    # Create a file by unpacking a poncho package.
    #
    # @param self       The current file object.
    # @param subfile    The file object to un-tgz.
        
    def __init__(self,subfile):
        self._file = vine_file_unponcho(vine_file_clone(subfile._file))


##
# \class FileUnstarch
#
# TaskVine File Starch Unpacker
#
# A wrapper to unpack a file in .sfx form.

class FileUnstarch(File):
    ##
    # Create a file by unpacking a starch package.
    #
    # @param self       The current file object.
    # @param subfile    The file object to un-tgz.
        
    def __init__(self,subfile):
        self._file = vine_file_unstarch(vine_file_clone(subfile._file))


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
    def __init__(self, command):
        self._task = None

        self._task = vine_task_create(command)
        if not self._task:
            raise Exception('Unable to create internal Task structure')

    def __del__(self):
        try:
            if self._task:
                vine_task_delete(self._task)
        except:
            #ignore exceptions, in case task has been already collected
            pass

    @staticmethod
    def _determine_file_flags(cache=False, unpack=False, watch=False, failure_only=False, success_only=False):
        flags = VINE_NOCACHE
        if cache:
            flags |= VINE_CACHE
        if watch:
            flags |= VINE_WATCH
        if failure_only:
            flags |= VINE_FAILURE_ONLY
        if success_only:
            flags |= VINE_SUCCESS_ONLY
        return flags

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
    # Indicate that the task would be optimally run on a given host.
    #
    # @param self       Reference to the current task object.
    # @param hostname   The hostname to which this task would optimally be sent.
    def set_preferred_host(self, hostname):
        return vine_task_set_preferred_host(self._task, hostname)

    ##
    # Add a local input file to a task.
    #
    # @param self          Reference to the current task object.
    # @param local_name    The name of the file on local disk or shared filesystem.
    # @param remote_name   The name of the file at the execution site.
    # @param cache         Whether the file should be cached at workers. Default is False.
    #
    # For example:
    # @code
    # # The following are equivalent
    # >>> task.add_input_file("/etc/hosts", cache = True)
    # >>> task.add_input_file("/etc/hosts", "hosts", cache = True)
    # @endcode
    def add_input_file(self, local_name, remote_name=None, cache=False ):

        # swig expects strings:
        if local_name:
            local_name = str(local_name)

        if remote_name:
            remote_name = str(remote_name)
        else:
            remote_name = os.path.basename(local_name)

        flags = Task._determine_file_flags(cache)
        return vine_task_add_input_file(self._task, local_name, remote_name, flags)


    ## Add an input url to a task.
    #
    # @param self          Reference to the current task object.
    # @param url           The url of the file to provide.
    # @param remote_name   The name of the file as seen by the task.
    # @param cache         Whether the file should be cached at workers (True/False)
    #
    # For example:
    # @code
    # >>> task.add_input_url("http://www.google.com/","google.txt",cache=True)
    # @endcode
    def add_input_url(self, url, remote_name, cache=False ):
        # swig expects strings
        if remote_name:
            remote_name = str(remote_name)

        if url:
            url = str(url)

        flags = Task._determine_file_flags(cache)
        return vine_task_add_input_url(self._task, url, remote_name, flags)


    ##
    # Add an input file produced by a mini task description.
    # The mini-task will be executed at the worker and produce
    # a cacheable file that can be shared among multiple tasks.
    # @param self           A task object.
    # @param mini_task      A task object that will produce the desired file.
    #                       The task object must generate a single output file named by @ref add_output_file.
    # @param remote_name    The name of the file as seen by the primary task.
    # @param cache         Whether the file should be cached at workers (True/False)
    #
    # For example:
    # @code
    # >>> # Create a mini-task:
    # >>> mini_task = Task("curl http://www.apnews.com > output.txt");
    # >>> mini_task.add_output_file("output.txt","output.txt");
    # >>> # Attach the output of the mini-task as the input of a main task:
    # >>> task.add_input_mini_task(mini_task,"infile.txt",cache=True)
    # @endcode

    def add_input_mini_task(self, mini_task, remote_name, cache=False, failure_only=None):
        if remote_name:
            remote_name = str(remote_name)
        flags = Task._determine_file_flags(cache=cache, failure_only=failure_only)
        # The minitask must be duplicated, because the C object becomes "owned"
        # by the parent task and will be deleted when the parent task goes away.
        copy_of_mini_task = vine_task_clone(mini_task._task)
        return vine_task_add_input_mini_task(self._task, copy_of_mini_task, remote_name, flags)

    ##
    # Add any input object to a task.
    #
    # @param self           Reference to the current task object.
    # @param file           A file object of class @ref File, such as @ref FileLocal, @ref FileBuffer, @ref FileURL, @ref FileMiniTask, @ref FileUntar, @FileUntgz.
    # @param remote_name    The name of the file at the execution site.
    # @param cache         Whether the file should be cached at workers (True/False)
    #
    # For example:
    # @code
    # >>> file = FileUntgz(FileURL(http://somewhere.edu/data.tgz))
    # >>> task.add_input(file,"data",cache=True)
    # @endcode
    def add_input(self, file, remote_name, cache=None, failure_only=None):
        # SWIG expects strings
        remote_name = str(remote_name)
        flags = Task._determine_file_flags(cache=cache, failure_only=None)
        copy_of_file = vine_file_clone(file._file)
        return vine_task_add_input(self._task, copy_of_file, remote_name, flags)


    ##
    # Add an empty directory to the task.
    # @param self           Reference to the current task object.
    # @param remote_name    The name of the directory at the remote execution site.
    def add_empty_dir(self, remote_name=None ):
        if remote_name:
            remote_name = str(remote_name)

        return vine_task_add_empty_dir(task,remote_name);

    ##
    # Add an input buffer to the task.
    #
    # @param self           Reference to the current task object.
    # @param buffer         The contents of the buffer to pass as input.
    # @param remote_name    The name of the remote file to create.
    # @param flags          May take the same values as @ref add_file.
    # @param cache          Whether the file should be cached at workers (True/False)
    def add_input_buffer(self, buffer, remote_name, cache=False ):
        if remote_name:
            remote_name = str(remote_name)
        flags = Task._determine_file_flags(cache)
        return vine_task_add_input_buffer(self._task, buffer, len(buffer), remote_name, flags)

    ##
    # Add a local output file to a task
    #
    # @param self          Reference to the current task object.
    # @param local_name    The name of the file on local disk or shared filesystem.
    # @param remote_name   The name of the file at the execution site.
    # @param cache         Whether the file should be cached at workers. Default is False.
    # @param watch         Watch the output file and send back changes as the task runs.
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs). Default is False.
    # @param success_only  For output files, whether the file should be retrieved only when the task succeeds. Default is False.
    #
    # For example:
    # @code
    # # The following are equivalent
    # >>> task.add_input_file("/etc/hosts", cache = True)
    # >>> task.add_input_file("/etc/hosts", "hosts", cache = True)
    # @endcode
    def add_output_file(self, local_name, remote_name=None, cache=False, watch=False, failure_only=False, success_only=False):
        if local_name:
            local_name = str(local_name)

        if remote_name:
            remote_name = str(remote_name)
        else:
            remote_name = os.path.basename(local_name)

        flags = Task._determine_file_flags(cache=cache, watch=watch, failure_only=failure_only, success_only=success_only)
        return vine_task_add_output_file(self._task, local_name, remote_name, flags)

    ##
    # Add an output buffer to the task.
    #
    # @param self          Reference to the current task object.
    # @param buffer_name   The logical name of the output buffer.
    # @param remote_name   The name of the remote file to fetch.
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param watch         Watch the output file and send back changes as the task runs.
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs). Default is False.
    # @param success_only  For output files, whether the file should be retrieved only when the task succeeds. Default is False.
    def add_output_buffer(self, buffer_name, remote_name, cache=False, watch=False, failure_only=False, success_only=False):
        if buffer_name:
            buffer_name = str(buffer_name)
        if remote_name:
            remote_name = str(remote_name)
        flags = Task._determine_file_flags(cache=cache, watch=watch, failure_only=failure_only, success_only=success_only)
        return vine_task_add_output_buffer(self._task, buffer_name, remote_name, flags)


    ##
    # Get an output buffer of the task.
    #
    # @param self           Reference to the current task object.
    # @param buffer_name    The logical name of the output buffer.
    # @return               The bytes of the returned file.

    def get_output_buffer(self, buffer_name ):
        if buffer_name:
            buffer_name = str(buffer_name)

        return vine_task_get_output_buffer(self._task, buffer_name )

    ##
    # Get the length of an output buffer.
    #
    # @param self           Reference to the current task object.
    # @param buffer_name    The logical name of the output buffer.
    # @return               The length of the output buffer.

    def get_output_buffer_length(self, buffer_name ):
        if buffer_name:
            buffer_name = str(buffer_name)

        return vine_task_get_output_buffer_length(self._task, buffer_name )

    ##
    # Add any output object to a task.
    # 
    # @param self           Reference to the current task object.
    # @param file           A file object of class @ref File, such as @ref FileLocal or @ref FileBuffer.
    # @param remote_name    The name of the file at the execution site.
    # @param flags          May be zero to indicate no special handling, or any
    #                       of the @ref vine_file_flags_t or'd together The most common are:
    #                       - @ref VINE_NOCACHE (default)
    #                       - @ref VINE_CACHE
    # @param cache         Whether the file should be cached at workers (True/False)
    # @param failure_only  For output files, whether the file should be retrieved only when the task fails (e.g., debug logs).
    #
    # For example:
    # @code
    # >>> file = FileLocal("output.txt")
    # >>> task.add_output(file,"out")
    # @endcode

    def add_output(self, file, remote_name, flags=None, cache=None, failure_only=None):
        # SWIG expects strings
        remote_name = str(remote_name)
        flags = Task._determine_file_flags(flags, cache, failure_only)
        copy_of_file = vine_file_clone(file,_file)
        return vine_task_add_output(self._task, copy_of_file, remote_name, flags)

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
        return vine_set_snapshot_file(self._task, filename)



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
    # 'SUCCESS'
    # @endcode
    @property
    def result_string(self):
        return vine_result_string(vine_task_get_result(self._task))

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
    def get_metric(self,name):
        return vine_task_get_metric(self._task,name)

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
            raise RuntimeError("PythonTask is not available. The dill module is missing.")

        self._func_file = os.path.join(self._tmpdir, 'function_{}.p'.format(self._id))
        self._args_file = os.path.join(self._tmpdir, 'args_{}.p'.format(self._id))
        self._out_file = os.path.join(self._tmpdir, 'out_{}.p'.format(self._id))
        self._wrapper = os.path.join(self._tmpdir, 'pytask_wrapper.py'.format(self._id))

        self._pp_run = None
        self._env_file  = None

        self._serialize_python_function(func, args, kwargs)
        self._create_wrapper()

        self._command = self._python_function_command()

        self._output_loaded = False
        self._output = None

        super(PythonTask, self).__init__(self._command)
        self._add_IO_files()

    ##
    # returns the result of a python task as a python variable
    #
    # @param self	reference to the current python task object
    @property
    def output(self):
        if not self._output_loaded:
            if self.result == VINE_RESULT_SUCCESS:
                try:
                    with open(os.path.join(self._tmpdir, 'out_{}.p'.format(self._id)), 'rb') as f:
                        self._output = dill.load(f)
                except Exception as e:
                    self._output = e
            else:
                self._output = PythonTaskNoResult()
                print(self.std_output)
            self._output_loaded = True
        return self._output


    def set_environment(self, env_file):
        if env_file:
            self._env_file = env_file
            self._pp_run = shutil.which('poncho_package_run')

            if not self._pp_run:
                raise RuntimeError("Could not find poncho_package_run in PATH.")

            self._command = self._python_function_command()
            vine_task_set_command(self._task, self._command)

            self.add_input_file(self._env_file, cache=True)
            self.add_input_file(self._pp_run, cache=True)

    def __del__(self):
        try:
            if self._tmpdir and os.path.exists(self._tmpdir):
                shutil.rmtree(self._tmpdir)

        except Exception as e:
            if sys:
                sys.stderr.write('could not delete {}: {}\n'.format(self._tmpdir, e))


    def _serialize_python_function(self, func, args, kwargs):
        with open(self._func_file, 'wb') as wf:
            dill.dump(func, wf, recurse=True)
        with open(self._args_file, 'wb') as wf:
            dill.dump([args, kwargs], wf, recurse=True)


    def _python_function_command(self):
        if self._env_file:
            py_exec="python"
        else:
            py_exec=f"python{sys.version_info[0]}"

        command = '{py_exec} {wrapper} {function} {args} {out}'.format(
                py_exec=py_exec,
                wrapper=os.path.basename(self._wrapper),
                function=os.path.basename(self._func_file),
                args=os.path.basename(self._args_file),
                out=os.path.basename(self._out_file))

        if self._env_file:
            command = './{pprun} -e {tar} --unpack-to "$VINE_SANDBOX"/{unpack}-env {cmd}'.format(
                pprun=os.path.basename(self._pp_run),
                unpack=os.path.basename(self._env_file),
                tar=os.path.basename(self._env_file),
                cmd=command)

        return command


    def _add_IO_files(self):
        self.add_input_file(self._wrapper, cache=True)
        self.add_input_file(self._func_file, cache=False)
        self.add_input_file(self._args_file, cache=False)
        self.add_output_file(self._out_file, cache=False)


    ##
    # creates the wrapper script which will execute the function. pickles output.
    def _create_wrapper(self):
        with open(self._wrapper, 'w') as f:
            f.write(textwrap.dedent('''\
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

                print(exec_out)'''))




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
    # @param stats_log  The name of a file to write the queue's statistics log.
    # @param transactions_log  The name of a file to write the queue's transactions log.
    # @param debug_log  The name of a file to write the queue's debug log.
    # @param shutdown   Automatically shutdown workers when queue is finished. Disabled by default.
    # @param ssl        A tuple of filenames (ssl_key, ssl_cert) in pem format, or True.
    #                   If not given, then TSL is not activated. If True, a self-signed temporary key and cert are generated.
    #
    # @see vine_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=VINE_DEFAULT_PORT, name=None, shutdown=False, stats_log=None, transactions_log=None, debug_log=None, ssl=None):
        self._shutdown = shutdown
        self._taskvine = None
        self._stats = None
        self._stats_hierarchy = None
        self._task_table = {}

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
            raise ValueError('port should be a single integer, or a sequence of two integers')

        try:
            if debug_log:
                self.enable_debug_log(debug_log)
            self._stats = vine_stats()
            self._stats_hierarchy = vine_stats()

            ssl_key, ssl_cert = self._setup_ssl(ssl)
            self._taskvine = vine_ssl_create(port, ssl_key, ssl_cert)
            if not self._taskvine:
                raise Exception('Could not create queue on port {}'.format(port))

            if stats_log:
                self.enable_perf_log(stats_log)

            if transactions_log:
                self.enable_transactions_log(transactions_log)

            if name:
                vine_set_name(self._taskvine, name)
        except Exception as e:
            raise Exception('Unable to create internal taskvine structure: {}'.format(e))


    def _free_queue(self):
        try:
            if self._taskvine:
                if self._shutdown:
                    self.shutdown_workers(0)
                vine_delete(self._taskvine)
                self._taskvine = None
        except:
            #ignore exceptions, as we are going away...
            pass

    def __del__(self):
        self._free_queue()

    def _setup_ssl(self, ssl):
        if not ssl:
            return (None, None)

        if ssl is not True:
            return ssl

        (tmp, key) = tempfile.mkstemp(
                dir=staging_directory,
                prefix='key')
        os.close(tmp)
        (tmp, cert) = tempfile.mkstemp(
                dir=staging_directory,
                prefix='cert')
        os.close(tmp)

        cmd=f"openssl req -x509 -newkey rsa:4096 -keyout {key} -out {cert} x-sha256 -days 365 -nodes -batch".split()

        output=""
        try:
            output=subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError as e:
            print(f"could not create temporary SSL key and cert {e}.\n{output}")
            raise e
        return (key, cert)

    ##
    # Get the project name of the queue.
    # @code
    # >>> print(q.name)
    # @endcode
    @property
    def name(self):
        return vine_get_name(self._taskvine)

    ##
    # Get the listening port of the queue.
    # @code
    # >>> print(q.port)
    # @endcode
    @property
    def port(self):
        return vine_port(self._taskvine)

    ##
    # Get queue statistics.
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
    # Get worker hierarchy statistics.
    # @code
    # >>> print(q.stats_hierarchy)
    # @endcode
    # The fields in @ref stats_hierarchy can also be individually accessed through this call. For example:
    # @code
    # >>> print(q.stats_hierarchy.workers_busy)
    # @endcode
    @property
    def stats_hierarchy(self):
        vine_get_stats_hierarchy(self._taskvine, self._stats_hierarchy)
        return self._stats_hierarchy

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
    # Get queue information as list of dictionaries
    # @param self Reference to the current manager object
    # @param request One of: "queue", "tasks", "workers", or "categories"
    # For example:
    # @code
    # import json
    # tasks_info = q.status("tasks")
    # @endcode
    def status(self, request):
        info_raw = vine_status(self._work_queue, request)
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
            workers.append({
                'workers': int(s.workers),
                'cores': int(s.cores),
                'gpus': int(s.gpus),
                'memory': int(s.memory),
                'disk': int(s.disk)})
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

    ## Enables resource monitoring of tasks in the queue, and writes a summary
    #  per task to the directory given. Additionally, all summaries are
    #  consolidate into the file all_summaries-PID.log
    #
    #  Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
    #
    # @param self   Reference to the current manager object.
    # @param dirname    Directory name for the monitor output.
    # @param watchdog   If True (default), kill tasks that exhaust their declared resources.
    def enable_monitoring(self, dirname=None, watchdog=True):
        return vine_enable_monitoring(self._taskvine, dirname, watchdog)

    ## As @ref enable_monitoring, but it also generates a time series and a debug file.
    #  WARNING: Such files may reach gigabyte sizes for long running tasks.
    #
    #  Returns 1 on success, 0 on failure (i.e., monitoring was not enabled).
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
    # Enable disconnect slow workers functionality for a given queue for tasks in
    # the "default" category, and for task which category does not set an
    # explicit multiplier.
    #
    # @param self       Reference to the current manager object.
    # @param multiplier The multiplier of the average task time at which point to disconnect a worker; if less than 1, it is disabled (default).
    def enable_disconnect_slow_workers(self, multiplier):
        return vine_enable_disconnect_slow_workers(self._taskvine, multiplier)

    ##
    # Enable disconnect slow workers functionality for a given queue.
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
    # Determine whether there are any known tasks queued, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current manager object.
    def empty(self):
        return vine_empty(self._taskvine)

    ##
    # Determine whether the queue can support more tasks.
    #
    # Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
    #
    # @param self       Reference to the current manager object.
    def hungry(self):
        return vine_hungry(self._taskvine)

    ##
    # Set the worker selection scheduler for queue.
    #
    # @param self       Reference to the current manager object.
    # @param scheduler  One of the following schedulers to use in assigning a
    #                   task to a worker. See @ref vine_schedule_t for
    #                   possible values.
    def set_scheduler(self, scheduler):
        return vine_set_scheduler(self._taskvine, scheduler)

    ##
    # Set the order for dispatching submitted tasks in the queue.
    #
    # @param self       Reference to the current manager object.
    # @param order      One of the following schedulers to use in dispatching
    #                   submitted tasks to workers:
    #                   - @ref VINE_TASK_ORDER_FIFO
    #                   - @ref VINE_TASK_ORDER_LIFO
    def set_task_order(self, order):
        return vine_set_task_order(self._taskvine, order)

    ##
    # Change the project name for the given queue.
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
    # Change the project priority for the given queue.
    #
    # @param self       Reference to the current manager object.
    # @param priority   An integer that presents the priorty of this manager manager. The higher the value, the higher the priority.
    def set_priority(self, priority):
        return vine_set_priority(self._taskvine, priority)

    ## Specify the number of tasks not yet submitted to the queue.
    # It is used by vine_factory to determine the number of workers to launch.
    # If not specified, it defaults to 0.
    # vine_factory considers the number of tasks as:
    # num tasks left + num tasks running + num tasks read.
    # @param self   Reference to the current manager object.
    # @param ntasks Number of tasks yet to be submitted.
    def tasks_left_count(self, ntasks):
        return vine_set_tasks_left_count(self._taskvine, ntasks)

    ##
    # Specify the manager mode for the given queue.
    # (Kept for compatibility. It is no-op.)
    #
    # @param self   Reference to the current manager object.
    # @param mode   This may be one of the following values: VINE_MASTER_MODE_STANDALONE or VINE_MASTER_MODE_CATALOG.
    def set_manager_mode(self, mode):
        return vine_set_manager_mode(self._taskvine, mode)

    ##
    # See set_manager_mode
    def set_master_mode(self, mode):
        return vine_set_manager_mode(self._taskvine, mode)

    ##
    # Specify the catalog servers the manager should report to.
    #
    # @param self       Reference to the current manager object.
    # @param catalogs   The catalog servers given as a comma delimited list of hostnames or hostname:port
    def set_catalog_servers(self, hostname, port):
        return vine_set_catalog_servers(self._taskvine, catalogs)

    ##
    # Specify a debug log file that records the manager actions in detail.
    #
    # @param self     Reference to the current manager object.
    # @param logfile  Filename.
    def enable_debug_log(self, logfile):
        return vine_enable_debug_log(self._taskvine, logfile)

    ##
    # Specify a performance log file that records the cummulative stats of connected workers and submitted tasks.
    #
    # @param self     Reference to the current manager object.
    # @param logfile  Filename.
    def enable_perf_log(self, logfile):
        return vine_enable_perf_log(self._taskvine, logfile)

    ##
    # Specify a log file that records the states of tasks.
    #
    # @param self     Reference to the current manager object.
    # @param logfile  Filename.
    def enable_transactions_log(self, logfile):
        vine_enable_transactions_log(self._taskvine, logfile)

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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
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
            old_value = getattr(rm, k) # to raise an exception for unknown keys
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
    # Cancel task identified by its task_id and remove from the given queue.
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
    # Cancel task identified by its tag and remove from the given queue.
    #
    # @param self   Reference to the current manager object.
    # @param tag    The tag assigned to task using @ref set_tag.
    def cancel_by_task_tag(self, tag):
        task = None
        task_pointer = vine_cancel_by_task_tag(self._taskvine, tag)
        if task_pointer:
            task = self._task_table.pop(int(id))
        return task

    ##
    # Cancel all tasks of the given category and remove them from the queue.
    #
    # @param self   Reference to the current manager object.
    # @param tag    The tag assigned to task using @ref set_tag.
    def cancel_by_category(self, category):
        canceled_tasks = []
        ids_to_cancel = []

        for task in self._task_table.values():
            if task.category == category:
                ids_to_cancel.append(task.id)

        canceled_tasks =  [self.cancel_by_task_id(id) for id in ids_to_cancel]
        return canceled_tasks


    ##
    # Shutdown workers connected to queue.
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
    # Delete file from workers's caches.
    #
    # @param self   Reference to the current manager object.
    # @param local_name   Name of the file as seen by the manager.
    def invalidate_cache_file(self, local_name):
        if local_name:
            local_name = str(local_name)
        return vine_invalidate_cached_file(self._taskvine, local_name, VINE_FILE)

    ##
    # Change keepalive interval for a given queue.
    #
    # @param self     Reference to the current manager object.
    # @param interval Minimum number of seconds to wait before sending new keepalive
    #                 checks to workers.
    def set_keepalive_interval(self, interval):
        return vine_set_keepalive_interval(self._taskvine, interval)

    ##
    # Change keepalive timeout for a given queue.
    #
    # @param self     Reference to the current manager object.
    # @param timeout  Minimum number of seconds to wait for a keepalive response
    #                 from worker before marking it as dead.
    def set_keepalive_timeout(self, timeout):
        return vine_set_keepalive_timeout(self._taskvine, timeout)

    ##
    # Turn on manager capacity measurements.
    #
    # @param self     Reference to the current manager object.
    #
    def estimate_capacity(self):
        return vine_set_estimate_capacity_on(self._taskvine, 1)

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
    # - "hungry-minimum" Mimimum number of tasks to consider queue not hungry. (default=10)
    # - "wait-for-workers" Mimimum number of workers to connect before starting dispatching tasks. (default=0)
    # - "wait_retrieve_many" Parameter to alter how vine_wait works. If set to 0, vine_wait breaks out of the while loop whenever a task changes to VINE_TASK_DONE (wait_retrieve_one mode). If set to 1, vine_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
    # @param value The value to set the parameter to.
    # @return 0 on succes, -1 on failure.
    #
    def tune(self, name, value):
        return vine_tune(self._taskvine, name, value)

    ##
    # Submit a task to the queue.
    #
    # It is safe to re-submit a task returned by @ref wait.
    #
    # @param self   Reference to the current manager object.
    # @param task   A task description created from @ref taskvine::Task.
    def submit(self, task):
        if isinstance(task, RemoteTask):
            task.add_input_buffer(json.dumps(task._event), "infile")
        task_id = vine_submit(self._taskvine, task._task)
        self._task_table[task_id] = task
        return task_id
    
    ##
    # Submit a duty to install on all connected workers 
    #
    #
    # @param self   Reference to the current manager object.
    # @param task   A task description created from @ref taskvine::Task.
    # @param name   Name of the duty to be installed.
    def install_duty(self, task, name):
        vine_manager_install_duty(self._taskvine, task._task, "duty_coprocess:" + name)

    ##
    # Remove a duty from all connected workers
    #
    #
    # @param self   Reference to the current manager object.
    # @param name   Name of the duty to be removed.
    def remove_duty(self, name):
        vine_manager_remove_duty(self._taskvine, "duty_coprocess:" + name)

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
        task_pointer = vine_wait_for_tag(self._taskvine, tag, timeout)
        if task_pointer:
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
    # Maps a function to elements in a sequence using taskvine
    #
    # Similar to regular map function in python
    #
    # @param self       Reference to the current manager object.
    # @param fn         The function that will be called on each element
    # @param seq        The sequence that will call the function
    # @param chunk_size The number of elements to process at once

    def map(self, fn, array, chunk_size=1):
        size = math.ceil(len(array)/chunk_size)
        results = [None] * size
        tasks = {}

        for i in range(size):
            start = i*chunk_size
            end = start + chunk_size

            if end > len(array):
                p_task = PythonTask(map, fn, array[start:])
            else:
                p_task = PythonTask(map, fn, array[start:end])

            p_task.set_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i

        n = 0
        for i in range(size+1):
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
    def pair(self, fn, seq1, seq2, chunk_size=1, env=None):
        def fpairs(fn, s):
            results = []

            for p in s:
                results.append(fn(p))

            return results

        size = math.ceil((len(seq1) * len(seq2))/chunk_size)
        results = [None] * size
        tasks = {}
        task = []
        num = 0
        num_task = 0

        for item in itertools.product(seq1, seq2):

            if num == chunk_size:
                p_task = PythonTask(fpairs, fn, task)
                if env:
                    p_task.set_environment(env)

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
    # @param chunk_size The number of elements per Task (for tree reduc, must be greater than 1)

    def tree_reduce(self, fn, seq, chunk_size=2): 
        tasks = {}
        num_task = 0

        while len(seq) > 1:
            size = math.ceil(len(seq)/chunk_size)
            results = [None] * size
        
            for i in range(size):
                start = i*chunk_size
                end = start + chunk_size

                if end > len(seq):
                    p_task = PythonTask(fn, seq[start:])
                else:
                    p_task = PythonTask(fn, seq[start:end])

                p_task.set_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num_task += 1

            n = 0
            for i in range(size+1):
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
    # @param chunk_size The number of elements to process at once
    def remote_map(self, fn, array, coprocess, name, chunk_size=1):
        size = math.ceil(len(array)/chunk_size)
        results = [None] * size
        tasks = {}

        for i in range(size):
            start = i*chunk_size
            end = min(len(array), start+chunk_size)

            event = json.dumps({name : array[start:end]})
            p_task = RemoteTask(fn, event, coprocess)
            
            p_task.set_tag(str(i))
            self.submit(p_task)
            tasks[p_task.id] = i
               
        n = 0
        for i in range(size+1):
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
    # @param chunk_size The number of elements to process at once
    def remote_pair(self, fn, seq1, seq2, coprocess, name, chunk_size=1):
        size = math.ceil((len(seq1) * len(seq2))/chunk_size)
        results = [None] * size
        tasks = {}
        task = []
        num = 0
        num_task = 0

        for item in itertools.product(seq1, seq2):
            if num == chunk_size:
                event = json.dumps({name : task})
                p_task = RemoteTask(fn, event, coprocess)
                p_task.set_tag(str(num_task))
                self.submit(p_task)
                tasks[p_task.id] = num_task                
                num = 0
                num_task += 1
                task.clear()

            task.append(item)
            num += 1

        if len(task) > 0:
            event = json.dumps({name : task})
            p_task = RemoteTask(fn, event, coprocess)
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
    # @param chunk_size The number of elements per Task (for tree reduc, must be greater than 1)
    def remote_tree_reduce(self, fn, seq, coprocess, name, chunk_size=2): 
        tasks = {}
        num_task = 0
        
        while len(seq) > 1:
            size = math.ceil(len(seq)/chunk_size)
            results = [None] * size

            for i in range(size):
                start = i*chunk_size
                end = min(len(seq), start+chunk_size)

                event = json.dumps({name : seq[start:end]})
                p_task = RemoteTask(fn, event, coprocess)

                p_task.set_tag(str(i))
                self.submit(p_task)
                tasks[p_task.id] = num_task
                num_task += 1

            n = 0
            for i in range(size+1):

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
# \class RemoteTask
#
# TaskVine RemoteTask object
#
# This class represents a task specialized to execute remotely-defined functions at workers.

class RemoteTask(Task):
    ##
    # Create a new remote task specification.
    #
    # @param self       Reference to the current remote task object.
    # @param fn         The name of the function to be executed on the coprocess
    # @param coprocess  The name of the coprocess which has the function you wish to execute. The coprocess should have a name() method that returns this
    # @param
    # @param command    The shell command line to be exected by the task.
    # @param args       positional arguments used in function to be executed by task. Can be mixed with kwargs
    # @param kwargs	    keyword arguments used in function to be executed by task. 
    def __init__(self, fn, coprocess, *args, **kwargs):
        Task.__init__(self, fn)
        self._event = {}
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args
        Task.set_coprocess(self, "duty_coprocess:" + coprocess)
    ##
    # Specify function arguments. Accepts arrays and dictionarys. This overrides any arguments passed during task creation
    # @param self             Reference to the current remote task object
    # @param args             An array of positional args to be passed to the function
    # @param kwargs           A dictionary of keyword arguments to be passed to the function
    def set_fn_args(self, args=[], kwargs={}):
        self._event["fn_kwargs"] = kwargs
        self._event["fn_args"] = args
    ##
    # Specify how the remote task should execute
    # @param self                     Reference to the current remote task object
    # @param remote_task_exec_method  Can be one of "fork", "direct", or "thread". Fork creates a child process to execute the function, direct has the worker directly call the function, and thread spawns a thread to execute the function
    def set_exec_method(self, remote_task_exec_method):
        if remote_task_exec_method not in ["fork", "direct", "thread"]:
            print("Error, vine_exec_method must be one of fork, direct, or thread")
        self._event["remote_task_exec_method"] = remote_task_exec_method



# test

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
    # `manager_name` or, `manager_host_port` should be specified.
    # If factory_binary or worker_binary is not
    # specified, $PATH will be searched.
    def __init__(
            self, batch_type,
            manager_name=None,
            manager_host_port=None,
            factory_binary=None, worker_binary=None,
            log_file=os.devnull):

        self._config_file = None
        self._factory_proc = None
        self._log_file = log_file

        (tmp, self._error_file) = tempfile.mkstemp(
                dir=staging_directory,
                prefix='vine-factory-err-')
        os.close(tmp)

        self._opts = {}

        self._set_manager(manager_name, manager_host_port)
        self._opts['batch-type'] = batch_type
        self._opts['worker-binary'] = self._find_exe(worker_binary, 'vine_worker')
        self._opts['scratch-dir'] = None

        self._factory_binary = self._find_exe(factory_binary, 'vine_factory')

    def _set_manager(self, manager_name, manager_host_port):
        if not (manager_name or manager_host_port):
            raise ValueError('Either manager_name or, manager_host_port should be specified.')

        if manager_name and manager_host_port:
            raise ValueError('Master should be specified by a name, or by a host and port. Not both.')

        if manager_name:
            self._opts['manager-name'] = manager_name
            return

        if manager_host_port:
            try:
                (host, port) = [x for x in manager_host_port.split(':') if x]
                self._opts['manager-host'] = host
                self._opts['manager-port'] = port
            except (TypeError, ValueError):
                raise ValueError('manager_name is not of the form HOST:PORT')

    def _find_exe(self, path, default):
        if path is None:
            out = distutils.spawn.find_executable(default)
        else:
            out = path
        if out is None or not os.access(out, os.F_OK):
            raise OSError(
                errno.ENOENT,
                'Command not found',
                out or default)
        if not os.access(out, os.X_OK):
            raise OSError(
                errno.EPERM,
                os.strerror(errno.EPERM),
                out)
        return os.path.abspath(out)


    def __getattr__(self, name):
        if name[0] == '_':
            # For names that start with '_', immediately return the attribute.
            # If the name does not start with '_' we assume is a factory option.
            return object.__getattribute__(self, name)

        # original command line options use - instead of _. _ is required by
        # the naming conventions of python (otherwise - is taken as 'minus')
        name_with_hyphens = name.replace('_', '-')

        if name_with_hyphens in Factory._command_line_options:
            try:
                return object.__getattribute__(self, '_opts')[name_with_hyphens]
            except KeyError:
                raise KeyError("{} is a valid factory attribute, but has not been set yet.".format(name))
        else:
            raise AttributeError("{} is not a supported option".format(name))


    def __setattr__(self, name, value):
        # original command line options use - instead of _. _ is required by
        # the naming conventions of python (otherwise - is taken as 'minus')
        name_with_hyphens = name.replace('_', '-')

        if name[0] == '_':
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
                raise AttributeError('{} cannot be changed once the factory is running.'.format(name))
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

        args += ['--parent-death']
        args += ['--config-file', self._config_file]

        if self._opts['batch-type'] == 'local':
            self._opts['extra-options'] = self._opts.get('extra-options', '') + ' --parent-death'

        flags = [opt for opt in Factory._command_line_options if opt[0] != ":"]

        for opt in self._opts:
            if opt not in Factory._command_line_options:
                continue
            if opt in Factory._config_file_options:
                continue
            if self._opts[opt] is True:
                args.append("--{}".format(opt))
            else:
                args.append("--{}={}".format(opt, self._opts[opt]))

        if 'manager-host' in self._opts:
            args += [self._opts['manager-host'], self._opts['manager-port']]

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
            raise RuntimeError('Factory was already started')
        (tmp, self._config_file) = tempfile.mkstemp(
                dir=staging_directory,
                prefix='vine-factory-config-',
                suffix='.json')

        if not self.scratch_dir:
            self.scratch_dir = tempfile.mkdtemp(
                    dir=staging_directory,
                    prefix="vine-factory-scratch-")

        os.close(tmp)
        self._write_config()
        logfd = open(self._log_file, 'a')
        errfd = open(self._error_file, 'w')
        devnull = open(os.devnull, 'w')
        self._factory_proc = subprocess.Popen(
            self._construct_command_line(),
            stdin=devnull,
            stdout=logfd,
            stderr=errfd)
        devnull.close()
        logfd.close()
        errfd.close()

        # ugly... give factory time to read configuration file
        time.sleep(1)

        status = self._factory_proc.poll()
        if status:
            with open(self._error_file) as error_f:
                error_log = error_f.read()
                raise RuntimeError('Could not execute vine_factory. Exited with status: {}\n{}'.format(str(status), error_log))
        return self


    ##
    # Stop the factory process.
    def stop(self):
        if self._factory_proc is None:
            raise RuntimeError('Factory not yet started')
        self._factory_proc.terminate()
        self._factory_proc.wait()
        self._factory_proc = None
        os.unlink(self._config_file)
        os.unlink(self._error_file)
        self._config_file = None


    def __enter__(self):
         return self.start()


    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


    def __del__(self):
        if self._factory_proc is not None:
            self.stop()


    def _write_config(self):
        if self._config_file is None:
            return

        opts_subset = dict([(opt, self._opts[opt]) for opt in self._opts if opt in Factory._config_file_options ])
        with open(self._config_file, 'w') as f:
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
