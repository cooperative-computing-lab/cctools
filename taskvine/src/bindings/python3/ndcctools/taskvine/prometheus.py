# Copyright (C) 2026- The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import numbers
import re
import sys

PROMETHEUS_DEFAULT_PORT = 9090

_MANAGER_SKIP_KEYS = frozenset({
    "type",
    "project",
    "owner",
    "working_dir",
    "version",
    "manager_preferred_connection",
    "taskvine_uuid",
    "categories",
    "workers_blocked",
    "network_interfaces",
})

_CATEGORY_LABEL_KEY = "category"
_CATEGORY_SKIP_KEYS = frozenset({_CATEGORY_LABEL_KEY})
_CATEGORY_NESTED_KEYS = frozenset({
    "first_allocation",
    "max_allocation",
    "max_seen",
})


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


def _manager_families(manager_status):
    from prometheus_client.core import GaugeMetricFamily

    if not manager_status:
        return

    for key, value in manager_status.items():
        if key in _MANAGER_SKIP_KEYS:
            continue
        if _is_numeric(value):
            name = _sanitize_metric_name(f"vine_{key}")
            family = GaugeMetricFamily(name, name)
            family.add_metric([], value)
            yield family


def _category_families(categories):
    from prometheus_client.core import GaugeMetricFamily

    families = {}

    def add(category, name, value):
        if name not in families:
            families[name] = GaugeMetricFamily(
                name,
                name,
                labels=[_CATEGORY_LABEL_KEY],
            )
        families[name].add_metric([category], value)

    for category_status in categories:
        category = category_status.get(_CATEGORY_LABEL_KEY)
        if not category:
            continue

        for key, value in category_status.items():
            if key in _CATEGORY_SKIP_KEYS or key in _CATEGORY_NESTED_KEYS:
                continue
            if _is_numeric(value):
                add(category, _sanitize_metric_name(f"vine_category_{key}"), value)

        for nested_key in _CATEGORY_NESTED_KEYS:
            nested = category_status.get(nested_key)
            if not isinstance(nested, dict):
                continue
            for field, value in nested.items():
                if _is_numeric(value):
                    metric = _sanitize_metric_name(f"vine_category_{nested_key}_{field}")
                    add(category, metric, value)

    for family in families.values():
        yield family


class StatusCollector(object):
    def __init__(self, status_fn):
        self._status_fn = status_fn

    def collect(self):
        try:
            manager = self._status_fn("manager")
            categories = self._status_fn("categories")
        except Exception as e:
            print(f"prometheus status error: {e}", file=sys.stderr)
            return

        yield from _manager_families(manager[0] if manager else None)
        yield from _category_families(categories or [])


def start(port, status_fn):
    pc = require_prometheus_client()
    registry = pc.CollectorRegistry()
    collector = StatusCollector(status_fn)
    registry.register(collector)

    # rely on GIL to ensure thread safety as SWIG bindings do
    # not release the GIL during vine_wait calls.
    httpd, thread = pc.start_http_server(port, registry=registry)
    return httpd, registry, collector
