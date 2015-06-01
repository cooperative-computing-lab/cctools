# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver options module """

from weaver.logger  import D_OPTIONS
from weaver.stack   import CurrentOptions, WeaverOptions, stack_context_manager


# Options class

@stack_context_manager(D_OPTIONS, WeaverOptions)
class Options(object):
    """ Weaver Options class.

    When a :class:`Options` object is created it will use the specified
    parameters (i.e. ``cpu``, ``memory``, ``disk``) to record the options.  If
    any of these parameters are unset (i.e. ``None``) then the unset parameters
    will take the value of the current :class:`Options` object.  As such, a
    :class:`Options` object will inherit options in an hierarchical fashion.
    """
    def __init__(self, cpu=None, memory=None, disk=None, batch=None,
        local=None, collect=None, environment=None):
        # Set options such that specified parameters override current options.
        try:
            self.cpu = cpu or WeaverOptions.top().cpu
        except AttributeError:
            self.cpu = cpu
        try:
            self.memory = memory or WeaverOptions.top().memory
        except AttributeError:
            self.memory = memory
        try:
            self.disk = disk or WeaverOptions.top().disk
        except AttributeError:
            self.disk = disk
        try:
            self.batch = batch or WeaverOptions.top().batch
        except AttributeError:
            self.batch = batch
        try:
            self.local = local or WeaverOptions.top().local
        except AttributeError:
            self.local = local
        try:
            self.collect = collect or WeaverOptions.top().collect
        except AttributeError:
            self.collect = collect
        try:
            self.environment = environment or dict(WeaverOptions.top().environment)
        except AttributeError:
            self.environment = environment

        if self.environment is None:
            self.environment = {}

    def __str__(self):
        return 'Options(cpu={0}, memory={1}, disk={2}, batch={3}, local={4}, collect={5}, environment={6})'.format(
            self.cpu, self.memory, self.disk, self.batch, self.local, self.collect, self.environment)

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
