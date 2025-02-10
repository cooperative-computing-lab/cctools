# Copyright (C) 2025- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

##
# @namespace ndcctools.taskvine.compat
#
# TaskVine data intensive workflow framework - Python Interface
#
# TaskVine is a framework for building large scale distributed data intensive
# applications that run on clusters, clouds, grids, and similar distributed systems.
# A TaskVine application consists of a main program that creates a Manager object,
# and then submits Task objects that use File objects representing data sources.
# The manager distributes tasks across available workers and returns results to
# the main application.
#
# This module implements graph execution based on tuples (old dask task graphs).
#
# See the <a href=http://cctools.readthedocs.io/en/latest/taskvine>TaskVine Manual</a> for complete documentation.
#
# Recommended import statement:
#
# @code
# form ndcctools.taskvine.compat import DaskVine
# @endcode
#
import warnings
from packaging.version import Version

try:
    import dask

    vd = Version(dask.__version__)
    vr = Version("2024.12.0")

    if vd >= vr:
        warnings.warn("ndcctools.taskvine.compat only works with dask version < 2024.12.0")
except ImportError:
    pass


from .dask_executor import DaskVine
from .dask_dag import DaskVineDag

__all__ = [
    "DaskVine",
    "DaskVineDag",
]

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
