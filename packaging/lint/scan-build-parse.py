#!/usr/bin/env python3
"""
Parse a clang scan-build run's index.html into a stable, sorted list of
findings, one per line, formatted as `checker:file:line`.

This is used by packaging/lint/scan-build.sh both to write
packaging/lint/scan-build-suppressions.txt (the baseline) and to compare a
fresh run's findings against that baseline. Keeping the parsing in one
script means both call sites can never drift apart on how a finding's
identity is derived.

Usage:
    scan-build-parse.py <scan-build-output-dir>

<scan-build-output-dir> is the directory passed to `scan-build -o`; this
script finds the single timestamped run subdirectory inside it (scan-build
creates a new one per run) and parses its index.html.
"""

import html
import os
import re
import sys

ROW_RE = re.compile(
    r'<tr class="([^"]+)">'
    r'<td class="DESC">[^<]*</td>'
    r'<td class="DESC">[^<]*</td>'
    r'<td>([^<]*)</td>'
    r'<td class="DESC">[^<]*</td>'
    r'<td class="Q">(\d+)</td>',
    re.S,
)


def find_run_dir(output_dir):
    candidates = [
        os.path.join(output_dir, name)
        for name in sorted(os.listdir(output_dir))
        if os.path.isdir(os.path.join(output_dir, name))
    ]
    if not candidates:
        return None
    # scan-build names each run's directory with a timestamp; the most
    # recent one (last alphabetically, since the format is YYYY-MM-DD-...)
    # is the one we just produced.
    return candidates[-1]


def parse_index(index_path):
    with open(index_path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()

    findings = set()
    for checker, file_path, line in ROW_RE.findall(text):
        file_path = html.unescape(file_path).strip()
        if not file_path:
            continue
        findings.add(f"{checker}:{file_path}:{line}")

    return findings


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <scan-build-output-dir>", file=sys.stderr)
        return 2

    output_dir = sys.argv[1]

    if not os.path.isdir(output_dir):
        print(f"no such directory: {output_dir}", file=sys.stderr)
        return 2

    run_dir = find_run_dir(output_dir)
    if run_dir is None:
        # scan-build found nothing to report and didn't create a run
        # subdirectory at all -- zero findings, not an error.
        return 0

    index_path = os.path.join(run_dir, "index.html")
    if not os.path.isfile(index_path):
        print(f"no index.html in {run_dir}", file=sys.stderr)
        return 2

    findings = parse_index(index_path)
    for finding in sorted(findings):
        print(finding)

    return 0


if __name__ == "__main__":
    sys.exit(main())
