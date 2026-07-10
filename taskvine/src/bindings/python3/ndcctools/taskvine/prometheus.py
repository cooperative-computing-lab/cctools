# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import numbers
import re
import sys

def require_prometheus_client():
    try:
        import prometheus_client
        return prometheus_client
    except ImportError as e:
        raise ImportError(
            "prometheus_client is required when prometheus=True. "
            "Install with: pip install prometheus_client"
        ) from e


def _sanitize_metric_name(name):
    return re.sub(r"[^a-zA-Z0-9_:]", "_", name)


def _is_numeric(value):
    return isinstance(value, numbers.Real) and not isinstance(value, bool)


def _stats_families(stats):
    from prometheus_client.core import GaugeMetricFamily

    if not stats:
        return

    for key in dir(stats):
        if key.startswith("_"):
            continue
        value = getattr(stats, key)
        if _is_numeric(value):
            name = _sanitize_metric_name(f"vine_{key}")
            family = GaugeMetricFamily(name, name)
            family.add_metric([], value)
            yield family


class StatsCollector(object):
    def __init__(self, stats_fn):
        self._stats_fn = stats_fn

    def collect(self):
        try:
            stats = self._stats_fn()
        except Exception as e:
            print(f"prometheus stats error: {e}", file=sys.stderr)
            return

        yield from _stats_families(stats)


def start(port, stats_fn):
    pc = require_prometheus_client()
    registry = pc.CollectorRegistry()
    collector = StatsCollector(stats_fn)
    registry.register(collector)

    httpd, thread = pc.start_http_server(port, registry=registry)
    return httpd, registry, collector
