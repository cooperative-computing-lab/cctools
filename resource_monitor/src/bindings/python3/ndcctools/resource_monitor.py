# Copyright (C) 2022 The University of Notre Dame This software is distributed
# under the GNU General Public License.
# See the file COPYING for details.
#
##
# @namespace ndcctools.resource_monitor
#
# Resource monitoring tool for complex applications - Python interface.
#
# The resource_monitor provides an unprivileged way for systems
# to monitor the consumption of key resources (cores, memory, disk)
# of applications ranging from simple Python functions up to complex
# multi-process trees.  It provides measurement, logging, enforcement,
# and triggers upon various conditions.
# The objects and methods provided by this package correspond to the native
# C API in @ref category.h, rmonitor_poll.h, and rmsummary.h
#
# The SWIG-based Python bindings provide a higher-level interface that
# revolves around the following function/decorator and objects:
#
# - @ref resource_monitor::monitored
# - @ref resource_monitor::ResourceExhaustion
# - @ref resource_monitor::Category

import fcntl
import functools
import multiprocessing
import os
import signal
import struct
import tempfile
import threading

from .cresource_monitor import (  # noqa: F401
    CATEGORY_ALLOCATION_MODE_FIXED,
    CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT,
    CATEGORY_ALLOCATION_MODE_MIN_WASTE,
    D_RMON,
    MINIMONITOR_ADD_PID,
    MINIMONITOR_MEASURE,
    MINIMONITOR_REMOVE_PID,
    category_accumulate_summary,
    category_create,
    category_specify_allocation_mode,
    category_tune_bucket_size,
    category_update_first_allocation,
    cctools_debug,
    cctools_debug_config,
    cctools_debug_flags_set,
    rmonitor_measure_process,
    rmonitor_minimonitor,
    rmsummary,
    rmsummary_check_limits,
    rmsummary_create,
    rmsummary_delete,
    rmsummary_copy,
    rmsummary_get_snapshot,
    rmsummary_merge_max,
    rmsummaryArray_getitem,
    delete_rmsummaryArray,
)


def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)


cctools_debug_config('resource_monitor')


##
# Create a monitored version of a function.
# It can be used as a decorator, or called by itself.
#
# @param limits     Dictionary of resource limits to set. Available limits are:
# - wall_time:                 time spent during execution (seconds)
# - cpu_time:                  user + system time of the execution (seconds)
# - cores:                     peak number of cores used
# - cores_avg:                 number of cores computed as cpu_time/wall_time
# - max_concurrent_processes:  the maximum number of processes running concurrently
# - total_processes:           count of all of the processes created
# - virtual_memory:            maximum virtual memory across all processes (megabytes)
# - memory:                    maximum resident size across all processes (megabytes)
# - swap_memory:               maximum swap usage across all processes (megabytes)
# - bytes_read:                number of bytes read from disk
# - bytes_written:             number of bytes written to disk
# - bytes_received:            number of bytes read from the network
# - bytes_sent:                number of bytes written to the network
# - bandwidth:                 maximum network bits/s (average over one minute)
# - total_files:               total maximum number of files and directories of all the working directories in the tree
# - disk:                      size of all working directories in the tree (megabytes)
#
# @param callback Function to call every time a measurement is done. The arguments given to the function are
# - id:   Unique identifier for the function and its arguments.
# - name: Name of the original function.
# - step: Measurement step. It is -1 for the last measurement taken.
# - resources: Current resources measured.
# @param interval Maximum time in seconds between measurements.
# @param return_resources Whether to modify the return value of the function to a tuple of the original result and a dictionary with the final measurements.
# @code
# # Decorating a function:
# @monitored()
# def my_sum_a(lst):
#   return sum(lst)
#
# @monitored(return_resources = False, callback = lambda id, name, step, res: print('memory used', res['memory']))
# def my_sum_b(lst):
#   return sum(lst)
#
# >>> (result_a, resources) = my_sum_a([1,2,3])
# >>> print(result, resources['memory'])
# 6, 66
#
# >>> result_b = my_sum_b([1,2,3])
# memory used: 66
#
# >>> assert(result_a == result_b)
#
#
# # Wrapping the already defined function 'sum', adding limits:
# my_sum_monitored = monitored(limits = {'memory': 1024})(sum)
# try:
#   # original_result = sum(...)
#   (original_result, resources_used) = my_sum_monitored(...)
# except ResourceExhaustion as e:
#   print(e)
#   ...
#
# # Defining a function with a callback and a decorator.
# # In this example, we record the time series of resources used:
# import multiprocessing
# results_series = multiprocessing.Queue()
#
# def my_callback(id, name, step, resources):
#     results_series.put((step, resources))
#
# @monitored(callback = my_callback, return_resources = False):
# def my_function(...):
#   ...
#
# result = my_function(...)
#
# # print the time series
# while not results_series.empty():
#     try:
#         step, resources = results_series.get(False)
#         print(step, resources)
#     except multiprocessing.Empty:
#         pass
# @endcode
def monitored(limits=None, callback=None, interval=1, return_resources=True):
    def monitored_inner(function):
        return functools.partial(__monitor_function, limits, callback, interval, return_resources, function)
    return monitored_inner


##
# Exception raised when a function goes over the resources limits
class ResourceExhaustion(Exception):
    ##
    # @param self                Reference to the current object.
    # @param resources           Dictionary of the resources measured at the time of the exhaustion.
    # @param function            Function that caused the exhaustion.
    # @param fn_args             List of positional arguments to function that caused the exhaustion.
    # @param fn_kwargs           Dictionary of keyword arguments to function that caused the exhaustion.
    def __init__(self, resources, function, fn_args=None, fn_kwargs=None):
        limits = resources['limits_exceeded']
        ls = ["{limit}: {value}".format(limit=k, value=limits[k]) for k in rmsummary.list_resources() if limits[k] > -1]

        message = 'Limits broken: {limits}'.format(limits=','.join(ls))
        super(ResourceExhaustion, self).__init__(resources, function, fn_args, fn_kwargs)

        self.resources = resources
        self.function = function
        self.fn_args = fn_args
        self.fn_kwargs = fn_kwargs
        self.message = message

    def __str__(self):
        return self.message


class ResourceInternalError(Exception):
    pass


def __measure_update_to_peak(pid, old_summary=None):
    new_summary = rmonitor_measure_process(pid, 1)

    if old_summary is None:
        return new_summary

    rmsummary_merge_max(old_summary, new_summary)
    return old_summary


def __child_handler(child_finished, signum, frame):
    child_finished.set()


def _wrap_function(results, fun, args, kwargs):
    def fun_wrapper():
        try:
            import os
            import time
            pid = os.getpid()
            rm = __measure_update_to_peak(pid)
            start = time.time()
            result = fun(*args, **kwargs)
            __measure_update_to_peak(pid, rm)
            setattr(rm, 'wall_time', int((time.time() - start)))
            results.put((result, rm))
        except Exception as e:
            results.put((e, None))
    cctools_debug(D_RMON, "function wrapper created")
    return fun_wrapper


def __read_pids_file(pids_file):
    fcntl.flock(pids_file, fcntl.LOCK_EX)
    line = bytearray(pids_file.read())
    fcntl.flock(pids_file, fcntl.LOCK_UN)

    n = len(line) / 4
    if n > 0:
        ns = struct.unpack('!' + 'i' * int(n), line)
        for pid in ns:
            if pid > 0:
                rmonitor_minimonitor(MINIMONITOR_ADD_PID, pid)
            else:
                rmonitor_minimonitor(MINIMONITOR_REMOVE_PID, -pid)


_watchman_counter = 0


def _watchman(results_queue, limits, callback, interval, function, args, kwargs):
    try:
        # child_finished is set when the process running function exits
        child_finished = threading.Event()
        signal.signal(signal.SIGCHLD, functools.partial(__child_handler, child_finished))

        # result of function is eventually push into local_results
        local_results = multiprocessing.Queue()

        # process that runs the original function
        fun_proc = multiprocessing.Process(target=_wrap_function(local_results, function, args, kwargs))

        # unique name for this function invocation
        # fun_id = str(hash(json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True)))
        global _watchman_counter
        _watchman_counter += 1
        fun_id = str(hash(_watchman_counter))

        # convert limits to the structure the minimonitor accepts
        if limits:
            limits = rmsummary.from_dict(limits)

        # pids of processes created by fun_proc (if any) are written to pids_file
        pids_file = None
        try:
            # try python3 version first, which gets the 'buffering' keyword argument
            pids_file = tempfile.NamedTemporaryFile(mode='rb+', prefix='p_mon-', buffering=0)
        except TypeError:
            # on error try python2, which gets the 'bufsize' keyword argument
            pids_file = tempfile.NamedTemporaryFile(mode='rb+', prefix='p_mon-', bufsize=0)

        os.environ['CCTOOLS_RESOURCE_MONITOR_PIDS_FILE'] = pids_file.name

        cctools_debug(D_RMON, "starting function process")
        fun_proc.start()

        rmonitor_minimonitor(MINIMONITOR_ADD_PID, fun_proc.pid)

        # resources_now keeps instantaneous measurements, resources_max keeps the maximum seen
        resources_now = rmonitor_minimonitor(MINIMONITOR_MEASURE, 0)
        resources_max = rmsummary_copy(resources_now, 0)

        try:
            step = 0
            while not child_finished.is_set():
                step += 1
                # register/deregister new processes
                __read_pids_file(pids_file)

                resources_now = rmonitor_minimonitor(MINIMONITOR_MEASURE, 0)
                rmsummary_merge_max(resources_max, resources_now)

                rmsummary_check_limits(resources_max, limits)
                if resources_max.limits_exceeded is not None:
                    child_finished.set()
                else:
                    if callback:
                        callback(fun_id, function.__name__, step, _resources_to_dict(resources_now))
                child_finished.wait(timeout=interval)
        except Exception as e:
            fun_proc.terminate()
            fun_proc.join()
            raise e

        if resources_max.limits_exceeded is not None:
            fun_proc.terminate()
            fun_proc.join()
            results_queue.put({'result': None, 'resources': resources_max, 'resource_exhaustion': True})
        else:
            fun_proc.join()
            try:
                (fun_result, resources_measured_end) = local_results.get(True, 5)
            except Exception as e:
                e = ResourceInternalError("No result generated: %s", e)
                cctools_debug(D_RMON, "{}".format(e))
                raise e

            if resources_measured_end is None:
                raise fun_result

            rmsummary_merge_max(resources_max, resources_measured_end)
            results_queue.put({'result': fun_result, 'resources': resources_max, 'resource_exhaustion': False})

        if callback:
            callback(fun_id, function.__name__, -1, _resources_to_dict(resources_max))

    except Exception as e:
        cctools_debug(D_RMON, "error executing function process: {err}".format(err=e))
        results_queue.put({'result': e, 'resources': None, 'resource_exhaustion': False})


def _resources_to_dict(resources):
    d = resources.to_dict()
    try:
        if d['wall_time'] > 0:
            d['cores_avg'] = float(d['cpu_time']) / float(d['wall_time'])
    except KeyError:
        d['cores_avg'] = -1
    return d


def __monitor_function(limits, callback, interval, return_resources, function, *args, **kwargs):
    result_queue = multiprocessing.Queue()

    watchman_proc = multiprocessing.Process(target=_watchman, args=(result_queue, limits, callback, interval, function, args, kwargs))
    watchman_proc.start()
    watchman_proc.join()

    results = result_queue.get(True, 5)

    if not results['resources']:
        raise results['result']

    results['resources'] = _resources_to_dict(results['resources'])

    # hack: add value of gpus limits as the value measured:
    try:
        if limits:
            results['resources']['gpus'] = limits['gpus']
    except KeyError:
        pass

    if results['resource_exhaustion']:
        raise ResourceExhaustion(results['resources'], function.__name__, args, kwargs)

    if return_resources:
        return (results['result'], results['resources'])
    else:
        return results['result']


##
# Class to encapsule all the categories in a workflow.
#
# @code
# cs = Categories()
# cs.accumulate_summary( { 'category': 'some_category', 'wall_time': 60, 'cores': 1, ... } )
# print(cs.first_allocation(mode = 'throughput', category = 'some_category'))
# @endcode
#
class Categories:

    ##
    # Create an empty set of categories.
    # @param self                Reference to the current object.
    # @param all_categories_name Name of the general category that holds all of the summaries.
    def __init__(self, all_categories_name='(all)'):
        self.categories = {}
        self.all_categories_name = all_categories_name
        category_tune_bucket_size('category-steady-n-tasks', -1)

    ##
    # Returns a lists of the category categories.  List sorted lexicographicaly,
    # with the exception of self.all_categories_name, which it is always
    # the last entry.
    # @param self                Reference to the current object.
    def category_names(self):
        categories = list(self.categories.keys())
        categories.sort()
        categories.remove(self.all_categories_name)
        categories.append(self.all_categories_name)
        return categories

    ##
    # Compute and return the first allocations for the given category.
    # Note: wall_time needs to be defined in the resource summaries to be
    # considered in this optimization.
    #
    # @param self                Reference to the current object.
    # @param mode                Optimization mode. One of 'throughput', 'waste', or 'fixed'.
    # @param category            Name of the category
    #
    # @code
    # cs = Categories()
    # fa = cs.first_allocation(mode = 'throughput, category = 'some_category')
    # print(fa['cores'])
    # print(fa['memory'])
    # print(fa['disk'])
    # @endcode
    def first_allocation(self, mode, category):
        c = self._category(category)
        return c.first_allocation(mode)

    ##
    # Return the maximum resource values so far seen for the given category.
    #
    # @param self                Reference to the current object.
    # @param category            Name of the category
    #
    # @code
    # cs = Categories()
    # fa = cs.maximum_seen('some_category')
    # print(fa['cores'])
    # print(fa['memory'])
    # print(fa['disk'])
    # @endcode
    def maximum_seen(self, category):
        c = self._category(category)
        return c.maximum_seen()

    ##
    # Add the summary (a dictionary) to the respective category.
    # At least both the 'category' and 'wall_time' keys should be defined.
    #
    # @code
    # cs = Categories()
    # cs.accumulate_summary( { 'category': 'some_category', 'wall_time': 50, 'cores': 1, ... } )
    # @endcode
    #
    def accumulate_summary(self, summary):
        category = summary['category']

        if category == self.all_categories_name:
            raise ValueError("category '" + self.all_categories_name + "' used for individual category.")

        c = self._category(category)
        c.accumulate_summary(summary)

        c = self._category(self.all_categories_name)
        c.accumulate_summary(summary)

    ##
    # Return the waste (unit x time) that would be produced if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param category            Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    # @param allocation          Value of allocation to test.
    #
    def waste(self, category, field, allocation):
        c = self._category(category)
        return c.waste(field, allocation)

    ##
    # Return the percentage of wasted resources that would be produced if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param category                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    # @param allocation          Value of allocation to test.
    #
    def wastepercentage(self, category, field, allocation):
        c = self._category(category)
        return c.wastepercentage(field, allocation)

    ##
    # Return the throughput that would be obtained if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param category                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    # @param allocation          Value of allocation to test.
    #
    def throughput(self, category, field, allocation):
        c = self._category(category)
        return c.throughput(field, allocation)

    ##
    # Return the number of tasks that would be retried if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param category                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    # @param allocation          Value of allocation to test.
    #
    def retries(self, category, field, allocation):
        c = self._category(category)
        return c.retries(field, allocation)

    ##
    # Return the number of summaries in a particular category.
    #
    # @param self                Reference to the current object.
    # @param category                Name of the category
    #
    def count(self, category):
        c = self._category(category)
        return c.count()

    def _category(self, category):
        try:
            return self.categories[category]
        except KeyError:
            cat = Category(category)
            self.categories[category] = cat
            return cat


#
# Class to represent a single category.
#
# Internal class.
class Category:
    def __init__(self, category):
        self.category = category
        self._cat = category_create(category)
        self.summaries = []

    def allocation_mode(self, mode):
        if mode == 'fixed':
            category_specify_allocation_mode(self._cat, CATEGORY_ALLOCATION_MODE_FIXED)
        elif mode == 'waste':
            category_specify_allocation_mode(self._cat, CATEGORY_ALLOCATION_MODE_MIN_WASTE)
        elif mode == 'throughput':
            category_specify_allocation_mode(self._cat, CATEGORY_ALLOCATION_MODE_MAX_THROUGHPUT)
        else:
            raise ValueError('No such mode')

    def accumulate_summary(self, summary):
        r = rmsummary.from_dict(summary)
        self.summaries.append(dict(summary))
        category_accumulate_summary(self._cat, r, None)

    def retries(self, field, allocation):
        retries = 0
        for r in self.summaries:
            if allocation < r[field]:
                retries += 1
        return retries

    def count(self):
        return len(self.summaries)

    def usage(self, field):
        usage = 0
        for r in self.summaries:
            resource = r[field]
            wall_time = r['wall_time']
            usage += wall_time * resource
        return usage

    def waste(self, field, allocation):
        maximum = self.maximum_seen()[field]

        waste = 0
        for r in self.summaries:
            resource = r[field]
            wall_time = r['wall_time']
            if resource > allocation:
                waste += wall_time * (allocation + maximum - resource)
            else:
                waste += wall_time * (allocation - resource)
        return waste

    def wastepercentage(self, field, allocation):
        waste = self.waste(field, allocation)
        usage = self.usage(field)

        return (100.0 * waste) / (waste + usage)

    def throughput(self, field, allocation):
        maximum = self.maximum_seen()[field]
        maximum = float(maximum)

        tasks = 0
        total_time = 0
        for r in self.summaries:
            resource = r[field]
            wall_time = r['wall_time']

            if resource > allocation:
                tasks += 1
                total_time += 2 * wall_time
            else:
                tasks += maximum / allocation
                total_time += wall_time
        return tasks / total_time

    def first_allocation(self, mode):
        self.allocation_mode(mode)

        if mode == 'fixed':
            return self.maximum_seen()

        category_update_first_allocation(self._cat, None)
        return self._cat.first_allocation.to_dict()

    def maximum_seen(self):
        return self._cat.max_resources_seen.to_dict()


def rmsummary_snapshots(self):
    if self.snapshots_count < 1:
        return None

    snapshots = []
    for i in range(0, self.snapshots_count):
        snapshot = rmsummary_get_snapshot(self, i)
        snapshots.append(snapshot)
    return snapshots


rmsummary.snapshots = property(rmsummary_snapshots)
