# Copyright (C) 2025 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

"""Namespace for the C vine graph bindings and Python client."""

try:  # pragma: no cover - module only exists after building the SWIG bindings
    from . import vine_graph_capi
except ImportError:
    vine_graph_capi = None

from .vine_graph_client import VineGraphClient

__all__ = ["vine_graph_capi", "VineGraphClient"]
