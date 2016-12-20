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

from cResourceMonitor import *

import math


def set_debug_flag(*flags):
    for flag in flags:
        cctools_debug_flags_set(flag)

cctools_debug_config('ResourceMonitorPython')

##
# Class to encapsule all the categories in a workflow.
#
# @code
# cs = Categories()
# cs.accumulate_summary( { 'category': 'some_category', 'wall_time': 60, 'cores': 1, ... } )
# print cs.first_allocation('some_category')
# @endcode
#

class Categories:

    ##
    # Create an empty set of categories.
    # @param self                Reference to the current object.
    # @param default_mode        First allocation optimization mode: 'througput', 'waste', 'fixed'
    # @param all_categories_name Name of the general category that holds all of the summaries.
    def __init__(self, default_mode = 'throughput', all_categories_name = '(all)'):
        self.categories          = {}
        self.default_mode        = default_mode
        self.all_categories_name = all_categories_name
        category_tune_bucket_size('category-steady-n-tasks', -1)

    ##
    # Returns a lists of the category names.  List sorted lexicographicaly,
    # with the exception of @ref self.all_categories_name, which it is always
    # the last entry.
    # @param self                Reference to the current object.
    def category_names(self):
        names = self.categories.keys()
        names.sort( self._cmp_names )
        return names

    def _cmp_names(self, a, b):
        # like cmp, but send all_categories_name to the last position
        if a == self.all_categories_name:
            return  1
        if b == self.all_categories_name:
            return -1
        return cmp(a, b)

    ##
    # Compute and return the first allocations for the given category.
    # Note: wall_time needs to be defined in the resource summaries to be
    # considered in this optimization.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    #
    # @code
    # cs = Categories()
    # fa = cs.first_allocation('some_category')
    # print fa.cores
    # print fa.memory
    # print fa.disk
    # @endcode
    def first_allocation(self, name):
        c = self._category(name)
        return c.first_allocation()

    ##
    # Return the maximum resource values so far seen for the given category.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    #
    # @code
    # cs = Categories()
    # fa = cs.maximum_seen('some_category')
    # print fa.cores
    # print fa.memory
    # print fa.disk
    # @endcode
    def maximum_seen(self, name):
        c = self._category(name)
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
        name      = summary['category']
        wall_time = summary['wall_time']

        if name == self.all_categories_name:
            raise ValueError("category '" + self.all_categories_name + "' used for individual category.")

        c = self._category(name)
        c.accumulate_summary(summary)

        c = self._category(self.all_categories_name)
        c.accumulate_summary(summary)

    ##
    # Return the waste (unit x time) that would be produced if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    #
    def waste(self, name, field, allocation):
        c = self._category(name)
        return c.waste(field, allocation)

    ##
    # Return the percentage of wasted resources that would be produced if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    #
    def wastepercentage(self, name, field, allocation):
        c = self._category(name)
        return c.wastepercentage(field, allocation)

    ##
    # Return the throughput that would be obtained if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    #
    def throughput(self, name, field, allocation):
        c = self._category(name)
        return c.throughput(field, allocation)

    ##
    # Return the number of tasks that would be retried if the accumulated
    # summaries were run under the given allocation.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    # @param field               Name of the resource (e.g., cores, memory, or disk)
    #
    def retries(self, name, field, allocation):
        c = self._category(name)
        return c.retries(field, allocation)

    ##
    # Return the number of summaries in a particular category.
    #
    # @param self                Reference to the current object.
    # @param name                Name of the category
    #
    def count(self, name):
        c = self._category(name)
        return c.count()

    def _category(self, name):
        try:
            return self.categories[name]
        except KeyError:
            cat = Category(name, self.default_mode)
            self.categories[name] = cat
            return cat


#
# Class to represent a single category.
#
# Internal class.
class Category:
    def __init__(self, name, mode):
        self.name = name
        self._cat = category_create(name)
        self.allocation_mode(mode)
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
        self.mode = mode

    def accumulate_summary(self, summary):
        r = self._dict_to_rmsummary(summary)
        self.summaries.append(self._rmsummary_to_dict(r))
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

    def first_allocation(self):
        category_update_first_allocation(self._cat, None)
        return self._rmsummary_to_dict(self._cat.first_allocation)

    def maximum_seen(self):
        return self._rmsummary_to_dict(self._cat.max_resources_seen)

    def _dict_to_rmsummary(self, pairs):
        rm = rmsummary_create(-1)
        for k, v in pairs.iteritems():
            if k in ['category', 'command', 'taskid']:
                pass                          # keep it as string
            elif k in ['start', 'end', 'wall_time', 'cpu_time', 'bandwidth', 'bytes_read', 'bytes_written', 'bytes_sent', 'bytes_received']:
                v = int(float(v) * 1000000)             # to s->miliseconds, Mb->bytes, Mbs->bps
            elif k in ['cores_avg']:
                v = int(float(v) * 1000)                # to milicores, int
            else:
                v = int(math.ceil(float(v)))  # to int
            setattr(rm, k, v)
        return rm

    def _rmsummary_to_dict(self, rm):
        if not rm:
            return None

        d = {}

        for k in ['category', 'command', 'taskid', 'exit_type']:
            v = getattr(rm, k)
            if v:
                d[k] = v

        for k in ['signal', 'exit_status', 'last_error']:
            v = getattr(rm, k)
            if v != 0:
                d[k] = v

        for k in ['total_processes', 'max_concurrent_processes',
                'virtual_memory', 'memory', 'swap_memory', 
                'total_files',
                'cores', 'gpus']:
            v = getattr(rm, k)
            if v > -1:
                d[k] = v

        for  k in ['start', 'end', 'cpu_time', 'wall_time',
                'bandwidth', 'bytes_read', 'bytes_written', 'bytes_received', 'bytes_sent']:
            v = getattr(rm, k)
            if v > -1:
                d[k] = v/1000000.0         # to s, Mbs, MB.

        for k in ['cores_avg']:
            v = getattr(rm, k)
            if v > -1:
                d[k] = v/1000.0            # to cores

        return d

