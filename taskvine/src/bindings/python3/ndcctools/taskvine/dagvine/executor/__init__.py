# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import importlib
import sys
from . import vine_graph_capi
from . import vine_graph
from .vine_graph import (
    ExecutorGraph,
    VineGraphExecutor,
    _format_scheduler_keys_runner_payload,
    format_scheduler_keys_runner_payload,
)

# SWIG-generated vine_graph_capi.py does "import cvine"; wire the top-level name to the real module.
sys.modules.setdefault("cvine", importlib.import_module("ndcctools.taskvine.cvine"))

sys.modules.setdefault(__name__ + ".graph", vine_graph)

__all__ = [
    "vine_graph_capi",
    "VineGraphExecutor",
    "ExecutorGraph",
    "_format_scheduler_keys_runner_payload",
    "format_scheduler_keys_runner_payload",
]
