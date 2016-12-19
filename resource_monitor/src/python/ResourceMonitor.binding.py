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
    def __init__(self, default_mode = 'througput', all_categories_name = '(all)'):
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
    # @param maximum_node        Size of the maximum computational node. If missing, use maximum values from accumulated tasks.
    #
    # @code
    # cs = Categories()
    # fa = cs.first_allocation('some_category')
    # print fa.cores
    # print fa.memory
    # print fa.disk
    # @endcode
    def first_allocation(self, name, maximum_node = None):
        c = self._category(name)
        return c.first_allocation(maximum_node)

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


    def allocation_mode(self, mode):
        if mode == 'fixed':
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_FIXED)
        elif mode == 'waste':
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE)
        elif mode == 'througput':
            category_specify_allocation_mode(self._cat, WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT)
        else:
            raise 'No such mode'
        self.mode = mode

    def accumulate_summary(self, summary):
        r = self._dict_to_rmsummary(summary)
        category_accumulate_summary(self._cat, r, None)

    def first_allocation(self, name, maximum_node = None):
        rmax = None
        if(maximum_node):
            rmax = self._dict_to_rmsummary(maximum_node)
        category_update_first_allocation(self._cat, rmax)

        return self._cat.first_allocation

    def maximum_seen(self):
        return self._cat.max_resources_seen

    def _dict_to_rmsummary(self, pairs):
        rm = rmsummary_create(-1)
        for k, v in pairs.iteritems():
            setattr(rm, k, v)
        return rm

