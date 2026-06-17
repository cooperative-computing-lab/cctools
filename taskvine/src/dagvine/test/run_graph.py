import argparse
import shutil
import tempfile
from pathlib import Path

from runner import parse_cases, run_graph


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-G", "--graph", nargs="+")
    p.add_argument("--case", action="append", dest="cases")
    p.add_argument("--task-group", type=int, default=0)
    p.add_argument("--task-priority-mode", default="random")
    p.add_argument("--port", type=int, default=9100)
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--no-print-results", action="store_true")
    args = p.parse_args()

    if args.cases:
        cases = parse_cases(args.cases)
    elif args.graph:
        if len(args.graph) > 2:
            p.error("-G takes GRAPH [N]")
        n = int(args.graph[1]) if len(args.graph) == 2 else None
        cases = [(args.graph[0], n)]
    else:
        p.error("need -G or --case")

    root = Path(tempfile.mkdtemp(prefix="vine_graph-run-")) if len(cases) > 1 else None
    rc = 0
    try:
        for i, (g, n) in enumerate(cases):
            try:
                res = run_graph(
                    g,
                    n,
                    task_group=args.task_group,
                    port=args.port + i,
                    work_root=root,
                    tag=f"{i:02d}-{g}-{n or 'na'}",
                    timeout_s=args.timeout,
                    priority=args.task_priority_mode,
                )
            except Exception as e:
                rc = 1
                print(g, n, "fail:", e)
                continue
            if not args.no_print_results:
                print(g, n, res)
    finally:
        if root:
            shutil.rmtree(root, ignore_errors=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
