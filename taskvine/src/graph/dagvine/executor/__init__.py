# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import importlib
import sys

# SWIG-generated graph_capi.py does "import cvine"; wire the top-level name to the real module.
sys.modules.setdefault("cvine", importlib.import_module("ndcctools.taskvine.cvine"))

from . import graph_capi
from .graph import ExecutorGraph, format_scheduler_keys_runner_payload


__all__ = ["graph_capi", "ExecutorGraph", "format_scheduler_keys_runner_payload"]
