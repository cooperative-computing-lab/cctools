import ndcctools.taskvine
import sys

from warnings import warn

msg = f"'import {__name__}' is deprecated. Please instead use: 'import ndcctools.{__name__}'"
warn(msg, DeprecationWarning, stacklevel=2)

sys.modules["taskvine"] = ndcctools.taskvine
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
