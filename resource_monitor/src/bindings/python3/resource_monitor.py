import ndcctools.resource_monitor
import sys

from warnings import warn

msg = f"'import {__name__}' is deprecated. Please instead use: 'import ndcctools.{__name__}'"
warn(msg, DeprecationWarning, stacklevel=2)

sys.modules["resource_monitor"] = ndcctools.resource_monitor
