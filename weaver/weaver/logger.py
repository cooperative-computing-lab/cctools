# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver logging module """

from weaver.util import parse_string_list, WeaverError

import sys
import time
import traceback


# Logging constants

D_ABSTRACTION = 'abstraction'
D_DATA        = 'data'
D_DATASET     = 'dataset'
D_ENGINE      = 'engine'
D_FUNCTION    = 'function'
D_LOGGER      = 'logger'
D_NEST        = 'nest'
D_OPTIONS     = 'options'
D_SCRIPT      = 'script'
D_UTIL        = 'utility'
D_USER        = 'user'
D_ALL         = 'all'


# Logger class

class Logger(object):
    """ Weaver logging class.

    Each instance maintains a set of systems (case in-sensitive) which have
    logging enabled.  If the appropriate system is enabled and a debug message
    is sent, it will be recorded to the logging stream.  Warning and fatal
    messages are always recorded regardless of whether or not that system is
    enabled.
    """

    #: Default log format
    DEFAULT_LOG_FORMAT = '[{flag}] {time} [{system:<11}] {message}'

    def __init__(self, stream=None, log_format=None, exit_on_fatal=True):
        """
        :param stream:          Output stream used to record messages.
        :param log_format:      String used to format log messages.
        :param exit_on_fatal:   Whether or not to abort on fatal message.


        The log format may contain the following parameters:

        - **flag**:     String representing message type (ex. 'D', 'W', or 'F')
        - **system**:   Name of system message is from.
        - **time**:     Current time in seconds.
        - **asctime**:  Current time in string format.
        - **message**:  Message to record.

        """
        self.stream         = stream or sys.stderr
        self.log_format     = log_format or Logger.DEFAULT_LOG_FORMAT
        self.exit_on_fatal  = exit_on_fatal
        self.systems        = set()

    def enable(self, systems):
        """ Enable logging of specified `systems`. """
        for system in parse_string_list(systems):
            self.systems.add(system.lower())
            self.debug(D_LOGGER, 'enable %s' % system)

    def disable(self, systems):
        """ Disable logging of specified `systems`. """
        for system in parse_string_list(systems):
            try:
                self.debug(D_LOGGER, 'disable %s' % system)
                self.systems.remove(system.upper())
            except KeyError:
                pass

    def log(self, flag, system, message, stream=None):
        """ Record log message for specified `system` and `message`. """
        log_kwargs = {
            'flag'   : flag.upper(),
            'system' : system.upper(),
            'time'   : '%0.2f' % time.time(),
            'asctime': time.asctime(time.localtime(time.time())),
            'message': message
        }

        stream = stream or self.stream
        stream.write(self.log_format.format(**log_kwargs) + '\n')
        stream.flush()

    def debug(self, system, message):
        """ Record debug `message` if system is `enabled`. """
        if system.lower() in self.systems or D_ALL.lower() in self.systems:
            self.log('D', system, message)

    def fatal(self, system, message, print_traceback=False):
        """ Record fatal `message`.

        Print Python stack trace if enabled. Exit if `exit_on_fatal` is set,
        otherwise, throw :class:`~weaver.util.WeaverError`.
        """
        # Print fatal log message to stderr AND log stream
        if self.stream != sys.stderr:
            self.log('F', system, message, stream=sys.stderr)
        self.log('F', system, '***> ' + message + ' <***')

        if print_traceback:
            traceback.print_exc(file=self.stream)
        if self.exit_on_fatal:
            sys.exit(1)
        else:
            raise WeaverError(system, message)

    def warn(self, system, message):
        """ Record warning `message`. """
        self.log('W', system, message)

    def set_log_path(self, path):
        self.stream = open(path, 'w')


# Internal Weaver Logger instance
_WeaverLogger = Logger()


# Export Weaver Logger methods as functions
enable        = _WeaverLogger.enable
disable       = _WeaverLogger.disable
debug         = _WeaverLogger.debug
fatal         = _WeaverLogger.fatal
warn          = _WeaverLogger.warn
set_log_path  = _WeaverLogger.set_log_path

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
