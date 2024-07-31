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
import io


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

    ##
    # Return the path of the file if a regular file, else None.
    #
    # @param self       A file object.
    def source(self):
        if self._file:
            return cvine.vine_file_source(self._file)

    ##
    # Return the enum type of this file. (e.g., VINE_FILE, VINE_TEMP, etc.)
    # Typically used to return the contents of an output buffer.
    #
    # @param self       A file object.
    def type(self):
        if self._file:
            return cvine.vine_file_type(self._file)

    ##
    # Return the contents of a file object as a string.
    # Typically used to return the contents of an output buffer.
    #
    # @param self         A file object.
    # @param unserializer A function to interpret file contents (e.g. cloudpickle.load)
    def contents(self, unserializer=None):
        def identity_read(x):
            return x.read()

        if not unserializer:
            unserializer = identity_read

        ftype = self.type()

        if ftype == cvine.VINE_BUFFER:
            with io.BytesIO(cvine.vine_file_contents_as_bytes(self._file)) as f:
                return unserializer(f)
        elif ftype == cvine.VINE_FILE:
            with open(self.source(), "rb") as f:
                return unserializer(f)
        else:
            with io.BytesIO(cvine.vine_file_contents_as_bytes(self._file)) as f:
                return unserializer(f)
            raise ValueError("File does not have local contents", self.type())

    ##
    # Return the size of a file object, in bytes.
    #
    # @param self       A file object.
    def __len__(self):
        return cvine.vine_file_size(self._file)
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
