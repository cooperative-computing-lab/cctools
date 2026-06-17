import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import cloudpickle
import graph_templates as gt
import ndcctools.taskvine.dagvine.vine_graph as vine_graph_mod
from ndcctools.taskvine.dagvine import VineGraph

TEST_DIR = Path(__file__).resolve().parent


def parse_cases(specs):
    out = []
    for s in specs:
        name, _, n = s.strip().partition(":")
        out.append((name, None if not n else int(n)))
    return out


def _vine_worker():
    w = Path(sys.executable).parent / "vine_worker"
    return str(w) if w.is_file() else shutil.which("vine_worker")


def _run_vine_graph(graph, n, task_group, port, logs, tag, out_dir, ckpt_dir, priority):
    run_info = logs / tag
    if run_info.exists():
        shutil.rmtree(run_info)

    wf = gt.build(graph, n)
    targets = ["output"] if "output" in wf.task_dict else []

    def context_loader(graph_pkl):
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)
        return {"graph": cloudpickle.loads(graph_pkl)}

    vine_graph_mod.context_loader_func = context_loader
    try:
        cloudpickle.register_pickle_by_value(gt)
    except Exception:
        pass

    m = VineGraph([port, port], run_info_path=str(logs), run_info_template=tag)
    m.update_params(
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
        hoisting_modules=[gt],
        env_files={"./graph_templates.py": "graph_templates.py"},
    ) or {}


def run_graph(
    graph,
    n=None,
    task_group=0,
    port=9100,
    work_root=None,
    tag="run",
    timeout_s=120.0,
    priority="random",
):
    worker = _vine_worker()
    if not worker:
        raise RuntimeError("vine_worker not found")

    os.environ.setdefault("CATALOG_UPDATE_PROTOCOL", "udp")

    root = work_root or Path(tempfile.mkdtemp(prefix="vine_graph-run-"))
    delete_root = work_root is None
    ws = root / f"ws-{tag}"
    shutil.rmtree(ws, ignore_errors=True)
    ws.mkdir(parents=True)
    logs = root / "logs"
    out_d = root / "out" / tag
    ckpt = root / "ckpt" / tag
    for d in (logs, out_d, ckpt):
        d.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    proc = subprocess.Popen(
        [worker, "--idle-timeout", str(max(120, int(timeout_s) + 60)), "-s", str(ws), "127.0.0.1", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=env,
    )
    time.sleep(1.2)

    def on_alarm(signum, frame):
        raise TimeoutError(timeout_s)

    try:
        old = signal.signal(signal.SIGALRM, on_alarm)
        signal.setitimer(signal.ITIMER_REAL, timeout_s)
        try:
            os.chdir(TEST_DIR)
            return _run_vine_graph(graph, n, task_group, port, logs, tag, out_d, ckpt, priority)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(ws, ignore_errors=True)
        if delete_root:
            shutil.rmtree(root, ignore_errors=True)
