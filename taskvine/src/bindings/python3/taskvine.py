import cctools.taskvine
import sys

from warnings import warn

msg = f"'import {__name__}' is deprecated. Please instead use: 'import cctools.{__name__}'"
warn(msg, DeprecationWarning, stacklevel=2)

sys.modules["taskvine"] = cctools.taskvine
