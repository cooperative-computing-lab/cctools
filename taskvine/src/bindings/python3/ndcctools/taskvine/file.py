# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

##
# @namespace ndcctools.taskvine.file
#
# This module provides the @ref ndcctools.taskvine.file.File "File" class
# to represent all inputs and outputs of tasks.
#

from . import cvine


##
# @class ndcctools.taskvine.file.File
#
# TaskVine File Object
#
# The superclass of all TaskVine file types.
class File(object):
    def __init__(self, internal_file):
        self._file = internal_file

    def __bool__(self):
        # We need this because the len of some files is 0, which would evaluate
        # to false.
        return True

    def source(self):
        file_type = getattr(self._file, "type")
        if file_type == cvine.VINE_FILE:
            return getattr(self._file, "source")
        else:
            return None

    ##
    # Return the contents of a file object as a string.
    # Typically used to return the contents of an output buffer.
    #
    # @param self       A file object.
    def contents(self):
        return cvine.vine_file_contents(self._file)

    ##
    # Return the size of a file object, in bytes.
    #
    # @param self       A file object.
    def __len__(self):
        return cvine.vine_file_size(self._file)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
