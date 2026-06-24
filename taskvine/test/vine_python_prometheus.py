#! /usr/bin/env python

import socket
import sys
import time
import unittest.mock
import urllib.error
import urllib.request

import ndcctools.taskvine as vine


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def fetch_metrics(port):
    url = f"http://127.0.0.1:{port}/metrics"
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode("utf-8")


def test_manager_without_prometheus():
    with vine.Manager(port=0) as m:
        if m.prometheus_port is not None:
            raise Exception("prometheus_port should be None when prometheus is disabled")


def test_prometheus_missing_dependency():
    import ndcctools.taskvine.prometheus as vine_prometheus

    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "prometheus_client":
            raise ImportError("simulated missing prometheus_client")
        return real_import(name, *args, **kwargs)

    metrics_port = find_free_port()
    with unittest.mock.patch("builtins.__import__", side_effect=fake_import):
        try:
            vine.Manager(port=0, prometheus=metrics_port)
        except ImportError as e:
            if "prometheus_client" not in str(e):
                raise
        else:
            raise Exception("Expected ImportError when prometheus_client is missing")


def test_prometheus_metrics():
    try:
        import prometheus_client  # noqa: F401
    except ImportError:
        print("prometheus_client not installed; skipping metrics scrape test")
        return

    metrics_port = find_free_port()
    with vine.Manager(port=0, prometheus=metrics_port) as m:
        if m.prometheus_port != metrics_port:
            raise Exception(f"Expected prometheus_port {metrics_port}, got {m.prometheus_port}")

        body = None
        for _ in range(10):
            try:
                body = fetch_metrics(metrics_port)
                break
            except urllib.error.URLError:
                time.sleep(0.5)

        if body is None:
            raise Exception("Could not fetch Prometheus metrics")

        for needle in ("vine_tasks_waiting", "vine_workers_connected", "# TYPE", "# HELP"):
            if needle not in body:
                raise Exception(f"Expected {needle!r} in metrics output")

        before = body

        t = vine.Task("/bin/true")
        m.submit(t)

        body_after = None
        for _ in range(10):
            try:
                body_after = fetch_metrics(metrics_port)
                if body_after != before:
                    break
            except urllib.error.URLError:
                pass
            time.sleep(0.5)

        print(f"metrics retrieved:\n{body_after}")

        if body_after is None:
            raise Exception("Could not fetch Prometheus metrics after task submission")

        if "vine_tasks_waiting" not in body_after:
            raise Exception("Expected vine_tasks_waiting after task submission")

    time.sleep(0.5)

    try:
        fetch_metrics(metrics_port)
    except urllib.error.URLError:
        pass
    else:
        raise Exception("Metrics server should stop after manager is destroyed")


if __name__ == "__main__":
    test_manager_without_prometheus()
    test_prometheus_missing_dependency()
    test_prometheus_metrics()
    print("prometheus tests passed")
# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
