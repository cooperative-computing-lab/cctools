# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import taskvine.cvine as cvine
from .display import JupyterDisplay
from .file import File
from .task import (
    FunctionCall,
    LibraryTask,
    PythonTask,
    Task,
)
from .utils import get_c_constant

import atexit
import itertools
import json
import math
import os
import pathlib
import subprocess
import sys
import tempfile

try:
    from poncho import package_serverize
    poncho_available = True
except Exception:
    poncho_available = False


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
    # @param port       The port number to listen on. If zero, then a random port is chosen. A range of possible ports (low, hight) can be also specified instead of a single integer. Default is 9123
    # @param name       The project name to use.
    # @param shutdown   Automatically shutdown workers when manager is finished. Disabled by default.
    # @param run_info_path Directory to write log and staging files per run. If None, defaults to "vine-run-info"
    # @param ssl        A tuple of filenames (ssl_key, ssl_cert) in pem format, or True.
    #                   If not given, then TSL is not activated. If True, a self-signed temporary key and cert are generated.
    # @param status_display_interval Number of seconds between updates to the jupyter status display. None, or less than 1 disables it.
    #
    # @see vine_create    - For more information about environmental variables that affect the behavior this method.
    def __init__(self, port=cvine.VINE_DEFAULT_PORT, name=None, shutdown=False, run_info_path="vine-run-info", ssl=None, status_display_interval=None):
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
            self._set_port_range(lower, upper)
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

            self._stats = cvine.vine_stats()
            self._stats_hierarchy = cvine.vine_stats()

            ssl_key, ssl_cert = self._setup_ssl(ssl, run_info_path)
            self._taskvine = cvine.vine_ssl_create(port, ssl_key, ssl_cert)

            if ssl_key:
                self._using_ssl = True

            if not self._taskvine:
                raise Exception("Could not create manager on port {}".format(port))

            if name:
                cvine.vine_set_name(self._taskvine, name)

            self._update_status_display()
        except Exception:
            sys.stderr.write("Unable to create internal taskvine structure.")
            raise

    def _set_port_range(self, low_port, high_port):
        if low_port > high_port:
            raise TypeError("low_port {} should be smaller than high_port {}".format(low_port, high_port))
        os.environ["TCP_LOW_PORT"] = str(low_port)
        os.environ["TCP_HIGH_PORT"] = str(high_port)

    def _free_manager(self):
        try:
            if self._taskvine:
                if self._shutdown:
                    self.shutdown_workers(0)
                self._update_status_display(force=True)
                cvine.vine_delete(self._taskvine)
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

        path = pathlib.Path(run_info_path)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

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
        return cvine.vine_get_name(self._taskvine)

    ##
    # Get the listening port of the manager.
    # @code
    # >>> print(q.port)
    # @endcode
    @property
    def port(self):
        return cvine.vine_port(self._taskvine)

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
        return cvine.vine_get_runtime_path_log(self._taskvine, None)

    ##
    # Get the staging directory of the manager
    @property
    def staging_directory(self):
        return cvine.vine_get_runtime_path_staging(self._taskvine, None)

    ##
    # Get the caching directory of the manager
    @property
    def cache_directory(self):
        return cvine.vine_get_runtime_path_caching(self._taskvine, None)

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
        cvine.vine_get_stats(self._taskvine, self._stats)
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
        stats = cvine.vine_stats()
        cvine.vine_get_stats_category(self._taskvine, category, stats)
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
        info_raw = cvine.vine_get_status(self._taskvine, request)
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
        from_c = cvine.vine_summarize_workers(self._taskvine)

        count = 0
        workers = []
        while True:
            s = cvine.rmsummayArray_getitem(from_c, count)
            if not s:
                break
            workers.append({"workers": int(s.workers), "cores": int(s.cores), "gpus": int(s.gpus), "memory": int(s.memory), "disk": int(s.disk)})
            cvine.rmsummary_delete(s)
            count += 1
        cvine.delete_rmsummayArray(from_c)
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
    #                  - "fixed" Task fails (default).
    #                  - "max" If maximum values are
    #                  specified for cores, memory, disk, and gpus (e.g. via @ref
    #                  set_category_resources_max or @ref Task.set_memory),
    #                  and one of those resources is exceeded, the task fails.
    #                  Otherwise it is retried until a large enough worker
    #                  connects to the manager, using the maximum values
    #                  specified, and the maximum values so far seen for
    #                  resources not specified. Use @ref Task.set_retries to
    #                  set a limit on the number of times manager attemps
    #                  to complete the task.
    #                  - "min waste" As above, but
    #                  manager tries allocations to minimize resource waste.
    #                  - "max throughput" As above, but
    #                  manager tries allocations to maximize throughput.
    def set_category_mode(self, category, mode):
        if isinstance(mode, str):
            mode = get_c_constant(f"allocation_mode_{mode.replace(' ', '_')}")
        return cvine.vine_set_category_mode(self._taskvine, category, mode)

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
        return cvine.vine_enable_category_resource(self._taskvine, category, category, resource, autolabel)

    ##
    # Get current task state. See @ref vine_task_state_t for possible values.
    # @code
    # >>> print(q.task_state(task_id))
    # @endcode
    def task_state(self, task_id):
        return cvine.vine_task_state(self._taskvine, task_id)

    ##
    # Enables resource monitoring for tasks. The resources measured are
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
        return cvine.vine_enable_monitoring(self._taskvine, watchdog, time_series)

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
        return cvine.vine_enable_monitoring_full(self._taskvine, dirname, watchdog)

    ##
    # Enable P2P worker transfer functionality. Off by default
    #
    # @param self Reference to the current manager object.
    def enable_peer_transfers(self):
        return cvine.vine_enable_peer_transfers(self._taskvine)

    ##
    # Enable disconnect slow workers functionality for a given manager for tasks in
    # the "default" category, and for task which category does not set an
    # explicit multiplier.
    #
    # @param self       Reference to the current manager object.
    # @param multiplier The multiplier of the average task time at which point to disconnect a worker; if less than 1, it is disabled (default).
    def enable_disconnect_slow_workers(self, multiplier):
        return cvine.vine_enable_disconnect_slow_workers(self._taskvine, multiplier)

    ##
    # Enable disconnect slow workers functionality for a given manager.
    #
    # @param self       Reference to the current manager object.
    # @param name       Name of the category.
    # @param multiplier The multiplier of the average task time at which point to disconnect a worker; disabled if less than one (see @ref enable_disconnect_slow_workers)
    def enable_disconnect_slow_workers_category(self, name, multiplier):
        return cvine.vine_enable_disconnect_slow_workers_category(self._taskvine, name, multiplier)

    ##
    # Turn on or off draining mode for workers at hostname.
    #
    # @param self       Reference to the current manager object.
    # @param hostname   The hostname the host running the workers.
    # @param drain_mode If True, no new tasks are dispatched to workers at hostname, and empty workers are shutdown. Else, workers works as usual.
    def set_draining_by_hostname(self, hostname, drain_mode=True):
        return cvine.vine_set_draining_by_hostname(self._taskvine, hostname, drain_mode)

    ##
    # Determine whether there are any known tasks managerd, running, or waiting to be collected.
    #
    # Returns 0 if there are tasks remaining in the system, 1 if the system is "empty".
    #
    # @param self       Reference to the current manager object.
    def empty(self):
        return cvine.vine_empty(self._taskvine)

    ##
    # Determine whether the manager can support more tasks.
    #
    # Returns the number of additional tasks it can support if "hungry" and 0 if "sated".
    #
    # @param self       Reference to the current manager object.
    def hungry(self):
        return cvine.vine_hungry(self._taskvine)

    ##
    # Set the worker selection scheduler for manager.
    #
    # @param self       Reference to the current manager object.
    # @param scheduler  One of the following schedulers to use in assigning a
    #                   task to a worker. See @ref vine_schedule_t for
    #                   possible values.
    def set_scheduler(self, scheduler):
        return cvine.vine_set_scheduler(self._taskvine, scheduler)

    ##
    # Change the project name for the given manager.
    #
    # @param self   Reference to the current manager object.
    # @param name   The new project name.
    def set_name(self, name):
        return cvine.vine_set_name(self._taskvine, name)

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
        return cvine.vine_set_manager_preferred_connection(self._taskvine, mode)

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
        return cvine.vine_set_task_id_min(self._taskvine, minid)

    ##
    # Change the project priority for the given manager.
    #
    # @param self       Reference to the current manager object.
    # @param priority   An integer that presents the priorty of this manager manager. The higher the value, the higher the priority.
    def set_priority(self, priority):
        return cvine.vine_set_priority(self._taskvine, priority)

    ##
    # Specify the number of tasks not yet submitted to the manager.
    # It is used by vine_factory to determine the number of workers to launch.
    # If not specified, it defaults to 0.
    # vine_factory considers the number of tasks as:
    # num tasks left + num tasks running + num tasks read.
    # @param self   Reference to the current manager object.
    # @param ntasks Number of tasks yet to be submitted.
    def tasks_left_count(self, ntasks):
        return cvine.vine_set_tasks_left_count(self._taskvine, ntasks)

    ##
    # Specify the catalog servers the manager should report to.
    #
    # @param self       Reference to the current manager object.
    # @param catalogs   The catalog servers given as a comma delimited list of hostnames or hostname:port
    def set_catalog_servers(self, catalogs):
        return cvine.vine_set_catalog_servers(self._taskvine, catalogs)

    ##
    # Specify a directory to write logs and staging files.
    #
    # @param self     Reference to the current manager object.
    # @param dirname  A directory name
    def set_runtime_info_path(self, dirname):
        cvine.vine_set_runtime_info_path(dirname)

    ##
    # Add a mandatory password that each worker must present.
    #
    # @param self      Reference to the current manager object.
    # @param password  The password.
    def set_password(self, password):
        return cvine.vine_set_password(self._taskvine, password)

    ##
    # Add a mandatory password file that each worker must present.
    #
    # @param self      Reference to the current manager object.
    # @param file      Name of the file containing the password.

    def set_password_file(self, file):
        return cvine.vine_set_password_file(self._taskvine, file)

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
        rm = cvine.rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return cvine.vine_set_resources_max(self._taskvine, rm)

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
        rm = cvine.rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return cvine.vine_set_resources_min(self._taskvine, rm)

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
        rm = cvine.rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return cvine.vine_set_category_resources_max(self._taskvine, category, rm)

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
        rm = cvine.rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return cvine.vine_set_category_resources_min(self._taskvine, category, rm)

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
        rm = cvine.rmsummary_create(-1)
        for k in rmd:
            setattr(rm, k, rmd[k])
        return cvine.vine_set_category_first_allocation_guess(self._taskvine, category, rm)

    ##
    # Initialize first value of categories
    #
    # @param self     Reference to the current manager object.
    # @param rm       Dictionary indicating maximum values. See @ref Task.resources_measured for possible fields.
    # @param filename JSON file with resource summaries.

    def initialize_categories(self, filename, rm):
        return cvine.vine_initialize_categories(self._taskvine, rm, filename)

    ##
    # Cancel task identified by its task_id and remove from the given manager.
    #
    # @param self   Reference to the current manager object.
    # @param id     The task_id returned from @ref submit.
    def cancel_by_task_id(self, id):
        task = None
        task_pointer = cvine.vine_cancel_by_task_id(self._taskvine, id)
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
        task_pointer = cvine.vine_cancel_by_task_tag(self._taskvine, tag)
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
        return cvine.vine_workers_shutdown(self._taskvine, n)

    ##
    # Block workers running on host from working for the manager.
    #
    # @param self   Reference to the current manager object.
    # @param host   The hostname the host running the workers.
    def block_host(self, host):
        return cvine.vine_block_host(self._taskvine, host)

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
        return cvine.vine_block_host_with_timeout(self._taskvine, host, timeout)

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
            return cvine.vine_unblock_all(self._taskvine)
        return cvine.vine_unblock_host(self._taskvine, host)

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
        return cvine.vine_set_keepalive_interval(self._taskvine, interval)

    ##
    # Change keepalive timeout for a given manager.
    #
    # @param self     Reference to the current manager object.
    # @param timeout  Minimum number of seconds to wait for a keepalive response
    #                 from worker before marking it as dead.
    def set_keepalive_timeout(self, timeout):
        return cvine.vine_set_keepalive_timeout(self._taskvine, timeout)

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
    # - "wait_retrieve_many" Parameter to alter how vine_wait works. If set to 0, cvine.vine_wait breaks out of the while loop whenever a task changes to "task_done" (wait_retrieve_one mode). If set to 1, vine_wait does not break, but continues recieving and dispatching tasks. This occurs until no task is sent or recieved, at which case it breaks out of the while loop (wait_retrieve_many mode). (default=0)
    # @param value The value to set the parameter to.
    # @return 0 on succes, -1 on failure.
    #
    def tune(self, name, value):
        return cvine.vine_tune(self._taskvine, name, value)

    ##
    # Submit a task to the manager.
    #
    # It is safe to re-submit a task returned by @ref wait.
    #
    # @param self   Reference to the current manager object.
    # @param task   A task description created from @ref taskvine::Task.
    def submit(self, task):
        task.submit_finalize(self)
        task_id = cvine.vine_submit(self._taskvine, task._task)
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
        cvine.vine_manager_install_library(self._taskvine, task._task, task.library_name)

    ##
    # Remove a library from all connected workers
    #
    #
    # @param self   Reference to the current manager object.
    # @param name   Name of the library to be removed.
    def remove_library(self, name):
        del self._library_table[name]
        cvine.vine_manager_remove_library(self._taskvine, name)

    ##
    # Turn a list of python functions into a Library
    #
    # @param self            Reference to the current manager object.
    # @param name            Name of the Library to be created
    # @param function_list   List of all functions to be included in the library
    # @returns               A task to be used with @ref Manager.install_library.
    def create_library_from_functions(self, name, *function_list):
        # ensure poncho python library is available
        if not poncho_available:
            raise ModuleNotFoundError("The poncho module is not available. Cannot create Library.")
        # positional arguments are the list of functions to include in the library
        # create a unique hash of a combination of function names and bodies
        functions_hash = package_serverize.generate_functions_hash(function_list)

        # create path for caching library code and environment based on function hash
        library_cache_path = f"{self.cache_directory}/vine-library-cache/{functions_hash}"
        library_code_path = f"{library_cache_path}/library_code.py"
        library_env_path = f"{library_cache_path}/library_env.tar.gz"

        # library cache folder doesn't exist, create it
        pathlib.Path(library_cache_path).mkdir(mode=0o755, parents=True, exist_ok=True)
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
        if not poncho_available:
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
    #                   "wait_forever" to block until a task has completed.
    def wait(self, timeout="wait_forever"):
        if timeout == "wait_forever":
            timeout = get_c_constant("wait_forever")
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
    def wait_for_tag(self, tag, timeout="wait_forever"):
        if timeout == "wait_forever":
            timeout = get_c_constant("wait_forever")

        self._update_status_display()

        task_pointer = cvine.vine_wait_for_tag(self._taskvine, tag, timeout)
        if task_pointer:
            if self.empty():
                # if last task in queue, update display
                self._update_status_display(force=True)
            task = self._task_table[cvine.vine_task_get_id(task_pointer)]
            del self._task_table[cvine.vine_task_get_id(task_pointer)]
            return task
        return None

    ##
    # Similar to @ref wait, but guarantees that the returned task has the
    # specified task_id.
    #
    # This call will block until the timeout has elapsed.
    #
    # @param self       Reference to the current manager object.
    # @param task_id    Desired task_id. If -1, then it is equivalent to self.wait(timeout)
    # @param timeout    The number of seconds to wait for a completed task
    #                   before returning.
    def wait_for_task_id(self, task_id, timeout="wait_forever"):
        if timeout == "wait_forever":
            timeout = get_c_constant("wait_forever")

        task_pointer = cvine.vine_wait_for_task_id(self._taskvine, task_id, timeout)
        if task_pointer:
            task = self._task_table[cvine.vine_task_get_id(task_pointer)]
            del self._task_table[cvine.vine_task_get_id(task_pointer)]
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
        f = cvine.vine_declare_file(self._taskvine, path, flags)
        return File(f)

    ##
    # Remove file from workers, undeclare it at the manager.
    # Note that this does not remove the file's local copy at the manager, if any.
    #
    # @param self    The manager to register this file
    # @param file    The file object
    def remove_file(self, file):
        cvine.vine_remove_file(self._taskvine, file._file)

    ##
    # Declare an anonymous file has no initial content, but is created as the
    # output of a task, and may be consumed by other tasks.
    #
    # @param manager    The manager to register this file
    # @return A file object to use in @ref Task.add_input or @ref Task.add_output
    def declare_temp(self):
        f = cvine.vine_declare_temp(self._taskvine)
        return File(f)

    ##
    # Create a file object representing an empty directory.
    # This is very occasionally needed for applications that expect
    # certain directories to exist in the working directory, prior to producing output.
    # This function does not transfer any data to the task, but just creates
    # a directory in its working sandbox.  If you want to transfer an entire
    # directory worth of data to a task, use @ref Task.declare_file and give a
    # directory name. output of a task, and may be consumed by other tasks.
    #
    # @param manager    The manager to register this file
    # @return A file object to use in @ref Task.add_input or @ref Task.add_output
    def declare_empty_dir(self):
        f = cvine.vine_declare_empty_dir(self._taskvine)
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

        f = cvine.vine_declare_url(self._taskvine, url, flags)
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
        f = cvine.vine_declare_buffer(self._taskvine, buffer, flags)
        return File(f)

    ##
    # Declare a file created by executing a mini-task.
    #
    # @param self     The manager to register this file
    # @param minitask The task to execute in order to produce a file
    # @return A file object to use in @ref Task.add_input
    def declare_minitask(self, minitask, cache=False, peer_transfer=True):
        flags = Task._determine_file_flags(cache, peer_transfer)
        f = cvine.vine_declare_mini_task(self._taskvine, minitask._task, flags)

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
        f = cvine.vine_declare_untar(self._taskvine, tarball._file, flags)
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
        f = cvine.vine_declare_poncho(self._taskvine, package._file, flags)
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
        f = cvine.vine_declare_starch(self._taskvine, starch._file, flags)
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
        f = cvine.vine_declare_xrootd(self._taskvine, source, proxy_c, env_c, flags)
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
        f = cvine.vine_declare_chirp(self._taskvine, server, source, ticket_c, env_c, flags)
        return File(f)


def rmsummary_snapshots(self):
    if self.snapshots_count < 1:
        return None

    snapshots = []
    for i in range(0, self.snapshots_count):
        snapshot = cvine.rmsummary_get_snapshot(self, i)
        snapshots.append(snapshot)
    return snapshots


cvine.rmsummary.snapshots = property(rmsummary_snapshots)
