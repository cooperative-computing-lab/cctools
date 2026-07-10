# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import numbers
import re
import sys

# Numeric fields from struct vine_stats (see taskvine.h).
_STATS_FIELDS = (
    "workers_connected",
    "workers_init",
    "workers_idle",
    "workers_busy",
    "workers_able",
    "workers_joined",
    "workers_removed",
    "workers_released",
    "workers_idled_out",
    "workers_slow",
    "workers_blocked",
    "workers_lost",
    "tasks_waiting",
    "tasks_on_workers",
    "tasks_running",
    "tasks_with_results",
    "tasks_recovery",
    "tasks_submitted",
    "tasks_dispatched",
    "tasks_done",
    "tasks_failed",
    "tasks_successful",
    "tasks_cancelled",
    "tasks_exhausted_attempts",
    "time_when_started",
    "time_send",
    "time_receive",
    "time_send_good",
    "time_receive_good",
    "time_status_msgs",
    "time_internal",
    "time_polling",
    "time_application",
    "time_scheduling",
    "time_workers_execute",
    "time_workers_execute_good",
    "time_workers_execute_exhaustion",
    "bytes_sent",
    "bytes_received",
    "bandwidth",
    "capacity_tasks",
    "capacity_cores",
    "capacity_memory",
    "capacity_disk",
    "capacity_gpus",
    "capacity_instantaneous",
    "capacity_weighted",
    "total_cores",
    "total_memory",
    "total_disk",
    "total_gpus",
    "committed_cores",
    "committed_memory",
    "committed_disk",
    "committed_gpus",
    "max_cores",
    "max_memory",
    "max_disk",
    "max_gpus",
    "min_cores",
    "min_memory",
    "min_disk",
    "min_gpus",
    "inuse_cache",
)


def require_prometheus_client():
    try:
        import prometheus_client
        return prometheus_client
    except ImportError as e:
        raise ImportError(
            "prometheus_client is required when prometheus_port is set. "
            "Install with: pip install prometheus_client"
        ) from e


def _sanitize_metric_name(name):
    return re.sub(r"[^a-zA-Z0-9_:]", "_", name)


def _coerce_numeric(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, numbers.Real):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stats_families(stats):
    from prometheus_client.core import GaugeMetricFamily

    if not stats:
        return

    for key in _STATS_FIELDS:
        value = _coerce_numeric(getattr(stats, key))
        if value is None:
            continue
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
