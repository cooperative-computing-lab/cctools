# Copyright (C) 2022- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

##
# @package taskvine
#
# Python API for the TaskVine workflow framework.
#
# TaskVine is a framework for building large scale distributed data intensive
# applications that run on clusters, clouds, grids, and similar distributed systems.
# A TaskVine application consists of a main program that creates a @ref Manager object,
# and then submits @ref Task objects that use @ref File objects representing data sources.
# The manager distributes tasks across available workers and returns results to
# the main application.
#
# See the <a href=http://cctools.readthedocs.io/en/latest/taskvine>TaskVine Manual</a> for complete documentation.
#
# - @ref Manager
# - @ref Task / @ref PythonTask / @ref FunctionCall
# - @ref File
# - @ref Factory
#
# The objects and methods provided by this package correspond closely
# to the native C API.


from .manager import Manager
from .file import File
from .factory import Factory
from .task import (
    Task,
    PythonTask,
    PythonTaskNoResult,
    LibraryTask,
    FunctionCall,
)
from .utils import get_c_constant


__all__ = [
    "Manager",
    "File",
    "Factory",
    "LibraryTask",
    "FunctionCall",
    "Task",
    "PythonTask",
    "PythonTaskNoResult",
    "get_c_constant"
]
