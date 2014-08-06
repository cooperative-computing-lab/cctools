# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver stack module """

from weaver.logger  import debug

import sys

# Stack class

class Stack(list):
    """ Basic stack data structure. """

    def empty(self):
        """ Return whether or not the stack is empty. """
        return len(self) == 0

    def top(self):
        """ Return item on top of the stack. """
        try:
            return self[-1]
        except IndexError:
            return None

    def push(self, obj):
        """ Push object on top of stack and return object. """
        self.append(obj)
        return self[-1]

    def pop(self):
        """ Pop object from top of stack and return object. """
        obj = self[-1]
        del(self[-1])
        return obj


# Stack context manager decorator

def stack_context_manager(flag, stack):
    """ This decorator is used to convert a class into a context manager that
    pushes itself onto the ``stack`` upon entering, and pops itself when it
    exits. """
    def wrapper(klass):
        def enter(self):
            debug(flag, 'Setting {0} {1}'.format(flag.title(), self))
            return stack.push(self)

        def exit(self, type, value, traceback):
            stack.pop()
            debug(flag, 'Restored {0} {1}'.format(flag.title(), stack.top()))

        klass.__enter__ = enter
        klass.__exit__  = exit
        return klass
    return wrapper


# Internal Weaver stacks

WeaverAbstractions = Stack()

def CurrentAbstraction():
    return WeaverAbstractions.top()


WeaverNests = Stack()

def CurrentNest():
    """ Return current Weaver Nest. """
    return WeaverNests.top()


WeaverOptions = Stack()

def CurrentOptions():
    """ Return current Weaver Options.

    .. note::
        Script-level options will override local options.
    """
    from weaver.options import Options
    top = WeaverOptions.top() or Options()
    return Options(
        cpu    = CurrentScript().options.cpu    or top.cpu,
        memory = CurrentScript().options.memory or top.memory,
        disk   = CurrentScript().options.disk   or top.disk,
        batch  = CurrentScript().options.batch  or top.batch,
        local  = CurrentScript().options.local  or top.local)


WeaverScripts = Stack()

def CurrentScript():
    """ Return the current Weaver Script instance. """
    # Normally Weaver programs are executed under the context of
    # weaver.script.Script, However, for doctests or standalone scripts, an
    # initial Script is required for proper functioning.
    if WeaverScripts.empty():
        from weaver.script import Script
        WeaverScripts.push(Script(sys.argv[0]))
    return WeaverScripts.top()

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
