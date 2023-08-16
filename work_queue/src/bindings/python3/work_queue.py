import ndcctools.work_queue
import sys

from warnings import warn

msg = f"'import {__name__}' is deprecated. Please instead use: 'import ndcctools.{__name__}'"
warn(msg, DeprecationWarning, stacklevel=2)

sys.modules["work_queue"] = ndcctools.work_queue
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
