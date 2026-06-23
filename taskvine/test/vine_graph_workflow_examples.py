import argparse
import json
import os
import shutil
import signal
import sys
import tempfile
from pathlib import Path

import cloudpickle
import ndcctools.taskvine.vine_graph.vine_graph as vine_graph_mod
from ndcctools.taskvine.vine_graph import VineGraph
from ndcctools.taskvine.vine_graph.workflow import TaskOutputRef, Workflow

TEST_DIR = Path(__file__).resolve().parent
MAX_BRANCHES = 32


def add(*args):
    return sum(args)


def make_simple_graph():
    bg = Workflow()
    bg.add_task("S0", add, 1, 5)
    return bg


def make_chain_graph(chain_len=1, branches=1):
    chain_len = max(1, int(chain_len))
    branches = max(1, int(branches))
    bg = Workflow()
    for b in range(branches):
        head = f"C{b}_0"
        bg.add_task(head, add, 1)
        prev = head
        for i in range(1, chain_len):
            node = f"C{b}_{i}"
            bg.add_task(node, add, TaskOutputRef(prev))
            prev = node
    return bg


def make_chain_rich(n=1):
    n = max(1, int(n))
    bg = Workflow()
    if n == 1:
        bg.add_task("CR0", add, 1)
        return bg
    branch_count = min(MAX_BRANCHES, max(1, n // 8))
    base, extra = divmod(n, branch_count)
    for b in range(branch_count):
        size = base + (1 if b < extra else 0)
        k = f"B{b}_0"
        bg.add_task(k, add, 1)
        prev = k
        for i in range(1, size):
            nk = f"B{b}_{i}"
            bg.add_task(nk, add, TaskOutputRef(prev))
            prev = nk
    return bg


def make_individuals(n=1):
    n = max(1, int(n))
    bg = Workflow()
    keys = [f"I{i}" for i in range(n)]
    for k in keys:
        bg.add_task(k, add, 1)
    return bg


def make_trivial(n=1):
    bg = Workflow()
    for i in range(max(1, int(n))):
        bg.add_task(f"T{i}", add, 1)
    return bg


def _add_binary_tree(bg, prefix, n):
    last = (n - 2) // 2
    for i in range(last + 1, n):
        bg.add_task(f"{prefix}{i}", add, 1)
    for i in range(last, -1, -1):
        deps = [TaskOutputRef(f"{prefix}{2 * i + 1}")]
        if 2 * i + 2 < n:
            deps.append(TaskOutputRef(f"{prefix}{2 * i + 2}"))
        bg.add_task(f"{prefix}{i}", add, *deps)


def make_binary_tree(n=1):
    n = max(1, int(n))
    bg = Workflow()
    _add_binary_tree(bg, "BT", n)
    return bg


def make_binary_forest(n=None, *, branches=5, level=8):
    bg = Workflow()
    if n is not None:
        n = max(1, int(n))
        branches = max(1, min(n, MAX_BRANCHES))
        base, extra = divmod(n, branches)
        for b in range(branches):
            size = base + (1 if b < extra else 0)
            _add_binary_tree(bg, f"F{b}_", size)
    else:
        branches, level = max(1, branches), max(1, level)
        tree_size = 2**level - 1
        for b in range(branches):
            _add_binary_tree(bg, f"F{b}_", tree_size)
    return bg


def build_graph(name, n=None):
    if name == "simple":
        return make_simple_graph()
    if name == "chain":
        return make_chain_graph(max(1, n or 8))
    if name == "chain-branches":
        return make_chain_graph(max(1, n or 8), branches=4)
    if name == "chain-rich":
        return make_chain_rich(max(1, n or 1000))
    if name == "binary-forest":
        return make_binary_forest(n)
    if name == "individuals":
        return make_individuals(max(1, n or 1000))
    if name == "trivial":
        return make_trivial(max(1, n or 1000))
    if name == "binary-tree":
        return make_binary_tree(max(1, n or 1000))
    raise ValueError(name)


def parse_cases(specs):
    out = []
    for s in specs:
        name, _, n = s.strip().partition(":")
        out.append((name, None if not n else int(n)))
    return out


def _sink_keys(workflow):
    return sorted(k for k in workflow.task_dict if not workflow.children_of.get(k))


def _run_vine_graph(graph, n, task_group, port, port_file, logs, tag, out_dir, ckpt_dir, priority):
    run_info = logs / tag
    if run_info.exists():
        shutil.rmtree(run_info)

    wf = build_graph(graph, n)
    targets = _sink_keys(wf)

    def context_loader(graph_pkl):
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
        return {"graph": cloudpickle.loads(graph_pkl)}

    vine_graph_mod.context_loader_func = context_loader
    try:
        cloudpickle.register_pickle_by_value(sys.modules[__name__])
    except Exception:
        pass

    m = VineGraph(port=port, run_info_path=str(logs), run_info_template=tag)
    if port_file:
        Path(port_file).write_text(str(m.port))

    m.set_params(
        {
            "checkpoint-dir": str(ckpt_dir),
            "extra-task-output-size-mb": [0.0, 0.0],
            "extra-task-sleep-time": [0.0, 0.0],
            "output-dir": str(out_dir),
            "task-group": task_group,
            "task-priority-mode": priority,
            "wait-for-workers": 1,
        }
    )
    return m.run(
        wf,
        target_keys=targets,
        hoisting_modules=[sys.modules[__name__]],
        env_files={"./vine_graph_workflow_examples.py": "vine_graph_workflow_examples.py"},
    ) or {}


def run_graph(
    graph,
    n=None,
    task_group=0,
    port=0,
    port_file=None,
    work_root=None,
    tag="run",
    timeout_s=120.0,
    priority="random",
):
    root = work_root or Path(tempfile.mkdtemp(prefix="vine_graph-run-"))
    delete_root = work_root is None
    logs = root / "logs"
    out_d = root / "out" / tag
    ckpt = root / "ckpt" / tag
    for d in (logs, out_d, ckpt):
        d.mkdir(parents=True, exist_ok=True)

    def on_alarm(signum, frame):
        raise TimeoutError(timeout_s)

    try:
        old = signal.signal(signal.SIGALRM, on_alarm)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        try:
            os.chdir(TEST_DIR)
            return _run_vine_graph(graph, n, task_group, port, port_file, logs, tag, out_d, ckpt, priority)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old)
    finally:
        if delete_root:
            shutil.rmtree(root, ignore_errors=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("port_file", nargs="?")
    p.add_argument("-G", "--graph", nargs="+")
    p.add_argument("--case", action="append", dest="cases")
    p.add_argument("--task-group", type=int, default=0)
    p.add_argument("--task-priority-mode", default="random")
    p.add_argument("--port", type=int, default=0)
    p.add_argument("--result-file")
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
    results = {}
    try:
        for i, (g, n) in enumerate(cases):
            try:
                res = run_graph(
                    g,
                    n,
                    task_group=args.task_group,
                    port=args.port if args.port == 0 else args.port + i,
                    port_file=args.port_file,
                    work_root=root,
                    tag=f"{i:02d}-{g}-{n or 'na'}",
                    timeout_s=args.timeout,
                    priority=args.task_priority_mode,
                )
            except Exception as e:
                rc = 1
                print(g, n, "fail:", e)
                continue
            results[f"{g}:{'' if n is None else n}"] = res
            if not args.no_print_results:
                print(g, n, res)
    finally:
        if root:
            shutil.rmtree(root, ignore_errors=True)
    if args.result_file:
        Path(args.result_file).write_text(json.dumps(results, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
