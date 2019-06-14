# Copyright (C) 2016- The University of Notre Dame This software is distributed
# under the GNU General Public License.
# See the file COPYING for details.
#
## @package ResourceMonitorPython
#
# Python Work Queue bindings.
#
# The objects and methods provided by this package correspond to the native
# C API in @ref category.h, rmonitor_poll.h, and rmsummary.h
#
# The SWIG-based Python bindings provide a higher-level interface that
# revolves around the following object:
#
# - @ref ResourceMonitor::Category

import functools
import fcntl
import math
import multiprocessing
import os
import signal
import struct
import tempfile
import threading
import time

def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)

cctools_debug_config('resource_monitor')


# decorator to monitor functions.
def monitored(limits = None, callback = None):
    def monitored_inner(func):
        return functools.partial(monitor_function, limits, callback, func)
    return monitored_inner

# if x = function(*args, **kwargs), returns a function mfun  (x, r) = mfun(*args, **kwargs)
# where x is the orignal results, and r is the maximum resources consumed.
def make_monitored(function, limits = None, callback = None):
    return functools.partial(monitor_function, limits, callback, function)

def measure_update_to_peak(pid, old_summary = None):
    new_summary = rmonitor_measure_process(pid)

    if old_summary is None:
        return new_summary
    else:
        rmsummary_merge_max(old_summary, new_summary)
        return old_summary

def __child_handler(child_finished, signum, frame):
    child_finished.set()

def __wrap_function(results, fun, args, kwargs):
    def fun_wrapper():
        try:
            import os
            import time
            pid = os.getpid()
            rm = measure_update_to_peak(pid)
            start = time.time()
            result = fun(*args, **kwargs)
            measure_update_to_peak(pid, rm)
            setattr(rm, 'wall_time', int((time.time() - start)*1e6))
            results.put((result, rm))
        except Exception as e:
            results.put((e, None))
    cctools_debug(D_RMON, "function wrapper created")
    return fun_wrapper

def __read_pids_file(pids_file):
    fcntl.flock(pids_file, fcntl.LOCK_EX)
    line = bytearray(pids_file.read())
    fcntl.flock(pids_file, fcntl.LOCK_UN)

    n = len(line)/4
    if n > 0:
        ns = struct.unpack('!' + 'i' * int(n), line)
        for pid in ns:
            if pid > 0:
                rmonitor_minimonitor(MINIMONITOR_ADD_PID, pid)
            else:
                rmonitor_minimonitor(MINIMONITOR_REMOVE_PID, -pid)

def __watchman(results_queue, limits, callback, function, args, kwargs):
    try:
        # child_finished is set when the process running function exits
        child_finished = threading.Event()
        old_handler = signal.signal(signal.SIGCHLD, functools.partial(__child_handler, child_finished))

        # result of function is eventually push into local_results
        local_results = multiprocessing.Queue()

        # process that runs the original function
        fun_proc = multiprocessing.Process(target=__wrap_function(local_results, function, args, kwargs))

        # convert limits to the structure the minimonitor accepts
        if limits:
            limits = rmsummary.from_dict(limits)

        # pids of processes created by fun_proc (if any) are written to pids_file
        pids_file = tempfile.NamedTemporaryFile(mode='rb+', prefix='p_mon-', buffering=0)
        os.environ['CCTOOLS_RESOURCE_MONITOR_PIDS_FILE']=pids_file.name

        cctools_debug(D_RMON, "starting function process")
        fun_proc.start()

        rmonitor_minimonitor(MINIMONITOR_ADD_PID, fun_proc.pid)

        # resources_now keeps instantaneous measurements, resources_max keeps the maximum seen
        resources_now = rmonitor_minimonitor(MINIMONITOR_MEASURE, 0)
        resources_max = rmsummary_copy(resources_now)

        try:
            while not child_finished.is_set():
                # register/deregister new processes
                __read_pids_file(pids_file)

                resources_now = rmonitor_minimonitor(MINIMONITOR_MEASURE, 0)
                rmsummary_merge_max(resources_max, resources_now)

                rmsummary_check_limits(resources_max, limits);
                if resources_max.limits_exceeded is not None:
                    child_finished.set()
                else:
                    if callback:
                        callback(resources=resources_now.to_dict(), finished=False, resource_exhaustion=False)
                child_finished.wait(timeout = 1)
        except Exception as e:
            fun_proc.terminate()
            fun_proc.join()
            raise e

        if resources_max.limits_exceeded is not None:
            fun_proc.terminate()
            fun_proc.join()
            results_queue.put({ 'result': None, 'resources': resources_max, 'resource_exhaustion': True})
        else:
            fun_proc.join()
            (fun_result, resources_measured_end) = local_results.get(True, 5)
            if resources_measured_end is None:
                raise fun_result

            rmsummary_merge_max(resources_max, resources_measured_end)
            results_queue.put({ 'result': fun_result, 'resources': resources_max, 'resource_exhaustion': False})

        if callback:
            callback(resources=resources_max.to_dict(), finished=True, resource_exhaustion=resources_max.limits_exceeded is not None)

    except Exception as e:
        cctools_debug(D_FATAL, "error executing function process: {}".format(e))
        results_queue.put({'result': e, 'resources': None, 'resource_exhaustion': False})

def monitor_function(limits, callback, function, *args, **kwargs):
    result_queue = multiprocessing.Queue()

    #__watchman(result_queue, limits, callback, function, args, kwargs)
    #sys

    watchman_proc = multiprocessing.Process(target=__watchman, args=(result_queue, limits, callback, function, args, kwargs))
    watchman_proc.start()
    watchman_proc.join()

    results = result_queue.get(True, 5)

    if not results['resources']:
        raise results['result']
    else:
        results['resources'] = results['resources'].to_dict()

    if results['resource_exhaustion']:
        raise ResourceExhaustion(results['resources'], function, args, kwargs)

    return (results['result'], results['resources'])


class ResourceExhaustion(Exception):
    def __init__(self, resources, function, args = None, kwargs = None):
        self.resources = resources
        self.function  = function
        self.args      = args
        self.kwargs    = kwargs

    def __str__(self):
        r = self.resources
        l = r['limits_exceeded']
        ls = ["{}: {}".format(k, l[k]) for k in l.keys() if (l[k] > -1 and l[k] < r[k])]

        return 'Limits broken: {}'.format(','.join(ls))

##
# Class to encapsule all the categories in a workflow.
#
# @code
# cs = Categories()
# cs.accumulate_summary( { 'category': 'some_category', 'wall_time': 60, 'cores': 1, ... } )
# print cs.first_allocation(mode = 'throughput', category = 'some_category')
# @endcode
#

class Categories:
    ##
    # Create an empty set of categories.
    # @param self                Reference to the current object.
    # @param all_categories_name Name of the general category that holds all of the summaries.
    def __init__(self, all_categories_name = '(all)'):
        self.categories          = {}
        self.all_categories_name = all_categories_name
        category_tune_bucket_size('category-steady-n-tasks', -1)

    ##
    # Returns a lists of the category categories.  List sorted lexicographicaly,
    # with the exception of @ref self.all_categories_name, which it is always
    # the last entry.
    # @param self                Reference to the current object.
    def category_names(self):
        categories = self.categories.keys()
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
    # print fa['cores']
    # print fa['memory']
    # print fa['disk']
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
    # print fa['cores']
    # print fa['memory']
    # print fa['disk']
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
        category      = summary['category']
        wall_time = summary['wall_time']

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
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_FIXED)
        elif mode == 'waste':
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE)
        elif mode == 'throughput':
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT)
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
            resource  = r[field]
            wall_time = r['wall_time']
            usage    += wall_time * resource
        return usage

    def waste(self, field, allocation):
        maximum = self.maximum_seen()[field]

        waste = 0
        for r in self.summaries:
            resource  = r[field]
            wall_time = r['wall_time']
            if resource > allocation:
                waste += wall_time * (allocation + maximum - resource)
            else:
                waste += wall_time * (allocation - resource)
        return waste

    def wastepercentage(self, field, allocation):
        waste = self.waste(field, allocation)
        usage = self.usage(field)

        return (100.0 * waste)/(waste + usage)

    def throughput(self, field, allocation):
        maximum = self.maximum_seen()[field]
        maximum = float(maximum)

        tasks      = 0
        total_time = 0
        for r in self.summaries:
            resource  = r[field]
            wall_time = r['wall_time']

            if resource > allocation:
                tasks      += 1
                total_time += 2*wall_time
            else:
                tasks      += maximum/allocation
                total_time += wall_time
        return tasks/total_time

    def first_allocation(self, mode):
        self.allocation_mode(mode)

        if mode == 'fixed':
            return self.maximum_seen()
        else:
            category_update_first_allocation(self._cat, None)
            return resource_monitor.to_dict(self._cat.first_allocation)

    def maximum_seen(self):
        return resource_monitor.to_dict(self._cat.max_resources_seen)

