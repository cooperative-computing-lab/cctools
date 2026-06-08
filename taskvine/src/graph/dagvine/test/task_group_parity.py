import argparse
import shutil
import tempfile
from pathlib import Path

from runner import parse_cases, run_graph

CASES = [
    ("simple", None),
    ("chain", 1),
    ("chain", 2),
    ("chain", 3),
    ("chain", 8),
    ("chain", 32),
    ("chain", 128),
    ("chain-branches", 6),
    ("chain-branches", 16),
    ("individuals", 8),
    ("trivial", 24),
    ("binary-tree", 7),
    ("binary-tree", 31),
    ("binary-tree", 127),
    ("chain-rich", 30),
    ("chain-rich", 120),
    ("chain-rich", 500),
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--base-port", type=int, default=22100)
    p.add_argument("--case", action="append", dest="cases")
    args = p.parse_args()

    cases = parse_cases(args.cases) if args.cases else CASES
    root = Path(tempfile.mkdtemp(prefix="dagvine-parity-"))
    rc = 0
    try:
        for i, (g, n) in enumerate(cases):
            tag = f"{i:02d}-{g}-{n or 'na'}"
            kw = dict(graph=g, n=n, work_root=root, timeout_s=args.timeout, priority="fifo")
            try:
                r0 = run_graph(**kw, port=args.base_port + i * 2, task_group=0, tag="tg0-" + tag)
                r1 = run_graph(**kw, port=args.base_port + i * 2 + 1, task_group=1, tag="tg1-" + tag)
            except TimeoutError:
                rc = 1
                print(g, n, "timeout")
                continue
            except Exception as e:
                rc = 1
                print(g, n, "fail:", e)
                continue
            if r0 != r1:
                rc = 1
                print(g, n, "mismatch", r0, r1)
            else:
                print(g, n, "ok", r0)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
