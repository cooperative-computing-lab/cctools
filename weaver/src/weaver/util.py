# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver utility module """

from weaver.compat import compat_next, zip_longest

import functools
import itertools
import errno
import os


# Constants

D_UTIL = 'utility' # Same as in weaver.logger, but avoid circular import


# Cloneable mixin

class Cloneable(object):
    """ Mixin that allows objects to be `cloneable`.

    Cloneable objects provide a :meth:`clone` method that allows the user to
    create a copy of the current object instance, while updating it with new
    attributes.  This is primarily used to allow for chaining modifications to
    an object without affecting the original instance.
    """
    def clone(self, **kwargs):
        """ Create new cloned object instance.

        This is done by copying attributes of the original object, and
        adding/updating instance members based on the keyword arguments.
        """
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__ = self.__dict__.copy()
        clone.__dict__.update(kwargs)
        return clone


# Container class

class Container(object):
    """ Construct an object based on the keyword arguments.

    This class creates an object instance with members based on the initial
    keyword arguments.  It used primarily to convert a :class:`dict` into an
    object instance.
    """
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


# Stash class

@compat_next
class Stash(object):
    """ Stash class.

    Organizes files by splitting them across a hierarchy of directories.
    """
    #: Default Stash depth.
    DEPTH = 3
    #: Default Stash files per folder.
    FILES_PER_FOLDER = 2**14

    def __init__(self, root=None, depth=None, setup=False):
        self.root  = root  or os.path.join(os.curdir, '_Stash')
        self.depth = depth or Stash.DEPTH

        self.file_counter   = itertools.cycle(range(0, Stash.FILES_PER_FOLDER))
        self.folder_counter = itertools.count()
        self.file_number    = None
        self.folder_number  = None

        if self.depth <= 0:
            raise WeaverError(D_UTIL,
                'Stash depth must be > 0, given: {0}'.format(self.depth))

        if setup:
            Stash.setup_stash(self.root, self.depth)

    def __next__(self):
        """ Return path to next available Stash *path*.

        We are limited to 2^14 files per directory and 16^``self.depth``
        directories, so a total of 16^``self.depth`` * 2^14.  By default this
        is 67108864 files total.
        """
        self.file_number = next(self.file_counter)
        if self.file_number == 0:
            self.folder_number = next(self.folder_counter)

        template = '{0' + ':0{0}X'.format(self.depth) + '}{1:04X}'
        number   = template.format(self.folder_number, self.file_number)
        args     = [c for c in number[:self.depth]] + [number]
        path     = os.path.join(self.root, *args)

        if not os.path.exists(os.path.dirname(path)):
            make_directory(os.path.dirname(path))

        return path

    @staticmethod
    def setup_stash(root, depth):
        """ Recursively setup Stash hierarchy. """
        make_directory(root)

        if depth > 0:
            for i in range(16):
                Stash.setup_stash(os.path.join(root, str('%X' % i)), depth - 1)


# Weaver Error class

class WeaverError(Exception):
    """ Custom Weaver :class:`Exception` used throughout the package. """
    def __init__(self, system, message):
        Exception.__init__(self)
        self.system = system
        self.message = message

    def __str__(self):
        return '%s: %s' % (self.system, self.message)


# Argument parsing utilities

def parse_object_list(object_list):
    """ Return :func:`iterable` of objects.

    Functions that use this utility can take in a single value, or a (possibly
    nested) list of values as an argument.
    """
    if object_list is None:
        return []

    if isinstance(object_list, str) or not iterable(object_list):
        return [object_list]
    else:
        return flatten(object_list)

def parse_string_list(string_list):
    """ Return :func:`iterable` of strings.

    This is the same as :func:`parse_object_list` except it converts all
    objects to strings.
    """
    return (str(o) for o in parse_object_list(string_list))


# Iterable utilities

def chunks(iterator, n, padvalue=None):
    """ Split iterator into groups of size n and return new iterator. """
    return zip_longest(*[iter(iterator)]*n, fillvalue=padvalue)

def groups(iterator, n):
    """ Like chunks, but filter out None from sub-groups. """
    return (filter(lambda x: x is not None, i) for i in chunks(iterator, n))

def flatten(object_list):
    """ Flatten nested lists into a single sequence and return iterator. """
    for o in object_list:
        for i in parse_object_list(o):
            yield i


# Logical utilities

def all_thunks(d, thunks):
    """ Return ``True`` if all `thunks` return true. """
    return all([t(d) for t in thunks])

def any_thunks(d, thunks):
    """ Return ``True`` if any of the ``thunks`` returns ``True``. """
    return any([t(d) for t in thunks])


# Miscellaneous utilities

def get_username_or_id():
    """ Return username if available, otherwise UID as string. """
    try:
        return os.environ['USER']
    except ValueError:
        return str(os.getuid())


# Path utilities

def find_executable(executable, find_dirs=None):
    """
    Return absolute path of `executable` if found, otherwise raise
    :class:`WeaverError`.

    This class takes into account the ``$PATH`` environmental variable along
    with the current directory.  To search in additional directories, specify
    them in `find_dirs`.
    """
    if os.path.exists(executable):
        return normalize_path(executable)

    find_dirs = parse_string_list(find_dirs)
    find_dirs = itertools.chain(find_dirs,
                    [os.curdir] + os.environ['PATH'].split(':'))

    for dir_path in find_dirs:
        exe_path = os.path.join(dir_path, executable)
        if os.path.exists(exe_path):
            return exe_path

    raise WeaverError(D_UTIL,
        'Could not find executable: {0}'.format(executable))

def normalize_path(path, ref_path=None):
    """ Return normalized path.

    If path is absolute or no `ref_path` is specified, then return absolute
    path.  Otherwise, return relative path.
    """
    from weaver.stack import CurrentScript

    if not CurrentScript().normalize_paths:
        return path

    if os.path.isabs(path):
        return path

    if ref_path:
        return os.path.abspath(os.path.join(ref_path, path))
    else:
        return os.path.abspath(path)

def make_directory(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise WeaverError(D_UTIL,
                'Could not make directory {0}: {1}'.format(path, e))


# Type utilites

def iterable(obj):
    """ Return whether or not an object is iterable. """
    return hasattr(obj, '__iter__')

def type_str(obj, full=False):
    """ Return string representation of object's type. """
    if obj.__class__ == type:
        obj_type = repr(obj)
    else:
        obj_type = str(obj.__class__)

    if full:
        return obj_type.split("'")[1]
    else:
        return obj_type.split("'")[1].split(".")[-1]

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
