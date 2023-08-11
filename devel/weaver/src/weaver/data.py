# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

""" Weaver data module """

from weaver.logger  import D_DATA, debug
from weaver.stack   import CurrentNest, CurrentScript
from weaver.util    import iterable, normalize_path, parse_object_list, \
                           parse_string_list, WeaverError

import os


# File classes

class File(object):
    """ Weaver base File class. """

    STATS = {
        'uid'  : lambda p: os.stat(p).st_uid,
        'gid'  : lambda p: os.stat(p).st_gid,
        'size' : lambda p: os.stat(p).st_size,
        'atime': lambda p: os.stat(p).st_atime,
        'mtime': lambda p: os.stat(p).st_mtime,
    }

    def __init__(self, path, nest=None):
        self.path = str(path)   # __str__ method returns path to file
        self.nest = nest or CurrentNest()
        debug(D_DATA, 'Created File: {0}'.format(self.path))

    def __getattr__(self, name):
        try:
            value = self.STATS[name](self.path)
            setattr(self, name, value)
            return value
        except KeyError:
            raise AttributeError

    def __lt__(self, other):
        return str(self) < str(other)

    def __cmp__(self, other):
        return cmp(str(self), str(other))

    def __str__(self):
        return normalize_path(self.path, self.nest.work_dir)


MakeFileCache = {}

def MakeFile(object_or_path, nest=None):
    if isinstance(object_or_path, File):
        return object_or_path
    else:
        if nest is None:
            nest = CurrentNest()
        try:
            key = (object_or_path, nest)
            return MakeFileCache[key]
        except KeyError:
            value = File(object_or_path, nest)
            MakeFileCache[key] = value
            return value


# File argument parsers

def parse_input_list(input_list=None):
    """ Return an :func:`~weaver.util.iterable` object of input files.

    This just uses :func:`~weaver.util.parse_string_list` to parse the input
    and casts all the objects to :class:`File`.

    This means that `input_list` must be one of the following:

    1. ``None`` or ``[]`` for an empty list.
    2. A string object.
    3. An :func:`~weaver.util.iterable` object (ex. list, iterator, etc.).

    Where each individual element must represent an :class:`File`.
    """
    debug(D_DATA, 'Parsing input list')
    return [MakeFile(i) for i in parse_object_list(input_list)]


def parse_output_list(output_list=None, input_list=None):
    """ Return an :func:`~weaver.util.iterable` object of output files.

    If `output_list` is ``None``, then return ``[]``.  If `output_list` is a
    string template, then use it to generate a list of :class:`File`
    objects.  If `output_list` is already an :func:`~weaver.util.iterable`,
    then map :class:`File` to `output_list` and return it.

    This means that `output_list` must be one of the following:

    1. ``None`` to leave it to the caller to generate an output file object.
    2. A string object to be used as a template.
    3. An :func:`~weaver.util.iterable` object (ex. list, iterator, etc.).

    If `output_list` is a string template, then it may have the following
    fields:

    - `{fullpath}`, `{FULL}`         -- Full input file path.
    - `{basename}`, `{BASE}`         -- Base input file name.
    - `{fullpath_woext}`, `{FULLWE}` -- Full input file path without extension
    - `{basename_woext}`, `{BASEWE}` -- Base input file name without extension

    """
    debug(D_DATA, 'Parsing output list')
    if output_list is None:
        return []

    if isinstance(output_list, str) or isinstance(output_list, File):
        # If input list is empty or output list is not a format string, then
        # return list of single output file.
        # TODO: support single {stash}
        if not input_list or not '{' in str(output_list):
            return [MakeFile(output_list)]

        nest = CurrentNest()
        return [MakeFile(str(output_list).format(
                    fullpath       = input,
                    FULL           = input,
                    i              = '{0:05X}'.format(i),
                    NUMBER         = '{0:05X}'.format(i),
                    stash          = next(nest.stash) if '{stash}' in output_list else '',
                    fullpath_woext = os.path.splitext(input)[0],
                    FULL_WOEXT     = os.path.splitext(input)[0],
                    basename       = os.path.basename(input),
                    BASE           = os.path.basename(input),
                    basename_woext = os.path.splitext(os.path.basename(input))[0],
                    BASE_WOEXT     = os.path.splitext(os.path.basename(input))[0]))
                for i, input in enumerate(parse_string_list(input_list))]

    if iterable(output_list):
        return [MakeFile(o) for o in parse_object_list(output_list)]

    raise WeaverError(D_DATA,
        'Could not parse output argument: {0}'.format(output_list))

# vim: set sts=4 sw=4 ts=8 expandtab ft=python:
