# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver compatibility module """

# execfile function
# Add execfile to Python3 since it is deprecated.

try:
    execfile = execfile
except NameError:
    def execfile(path, _globals=None, _locals=None):
        """ Execute specified Python file using exec. """
        with open(path, 'rb') as fs:
            exec(fs.read(), _globals, _locals)


# callable function
# Add callable to Python3 since it is deprecated.

try:
    callable = callable
except NameError:
    callable = lambda f: hasattr(f, '__call__')


# getfuncname function
# Function name is different in Python3

def getfuncname(function):
    """ Return name of function. """
    try:
        return function.func_name # Python2
    except AttributeError:
        return function.__name__  # Python3


# map function
# Make Python2 map the same as imap (always want iterator rather than list).

try:
    from itertools import imap
    map = imap
except ImportError:
    map = map


# zip_longest function
# Make Python2 zip_longest the same as izip_longest.

try:
    from itertools import izip_longest
    zip_longest = izip_longest
except ImportError:
    from itertools import zip_longest
    zip_longest = zip_longest


# Next compatibility decorator
# Python3 uses __next__, while Python2 uses next.

def compat_next(original_class):
    """
    Alias next class method to __next__ to make Python3 style iterators
    compatible with Python2.
    """
    original_class.next = original_class.__next__
    return original_class

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
