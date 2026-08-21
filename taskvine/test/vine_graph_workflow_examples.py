import argparse
from collections import deque, namedtuple
from dataclasses import dataclass
import json
import os
import shutil
import signal
import sys
import tempfile
from pathlib import Path

import cloudpickle
import ndcctools.taskvine.vine_graph.vine_graph as vine_graph_mod
from ndcctools.taskvine.vine_graph import FileHandle, TaskHandle, TaskOutputHandle, VineGraph, Workflow

TEST_DIR = Path(__file__).resolve().parent
MAX_BRANCHES = 32


def add(*args):
    return sum(args)


def make_simple_graph():
    bg = Workflow()
    bg.add_task(add, 1, 5)
    return bg


def make_chain_graph(chain_len=1, branches=1):
    chain_len = max(1, int(chain_len))
    branches = max(1, int(branches))
    bg = Workflow()
    for b in range(branches):
        prev = bg.add_task(add, 1)
        for i in range(1, chain_len):
            prev = bg.add_task(add, prev.output())
    return bg


def make_chain_rich(n=1):
    n = max(1, int(n))
    bg = Workflow()
    if n == 1:
        bg.add_task(add, 1)
        return bg
    branch_count = min(MAX_BRANCHES, max(1, n // 8))
    base, extra = divmod(n, branch_count)
    for b in range(branch_count):
        size = base + (1 if b < extra else 0)
        prev = bg.add_task(add, 1)
        for i in range(1, size):
            prev = bg.add_task(add, prev.output())
    return bg


def make_individuals(n=1):
    n = max(1, int(n))
    bg = Workflow()
    for _ in range(n):
        bg.add_task(add, 1)
    return bg


def make_trivial(n=1):
    bg = Workflow()
    for _ in range(max(1, int(n))):
        bg.add_task(add, 1)
    return bg


def _add_binary_tree(bg, n):
    tasks = [None] * n
    last = (n - 2) // 2
    for i in range(last + 1, n):
        tasks[i] = bg.add_task(add, 1)
    for i in range(last, -1, -1):
        deps = [tasks[2 * i + 1].output()]
        if 2 * i + 2 < n:
            deps.append(tasks[2 * i + 2].output())
        tasks[i] = bg.add_task(add, *deps)


def make_binary_tree(n=1):
    n = max(1, int(n))
    bg = Workflow()
    _add_binary_tree(bg, n)
    return bg


def make_binary_forest(n=None, *, branches=5, level=8):
    bg = Workflow()
    if n is not None:
        n = max(1, int(n))
        branches = max(1, min(n, MAX_BRANCHES))
        base, extra = divmod(n, branches)
        for b in range(branches):
            size = base + (1 if b < extra else 0)
            _add_binary_tree(bg, size)
    else:
        branches, level = max(1, branches), max(1, level)
        tree_size = 2**level - 1
        for _ in range(branches):
            _add_binary_tree(bg, tree_size)
    return bg


CornerRecord = namedtuple("CornerRecord", ["left", "right"])


@dataclass
class CornerArguments:
    value: object
    nested: object


@dataclass(frozen=True)
class FrozenCornerArguments:
    value: object


@dataclass
class FileCornerArguments:
    left: object
    nested: object


class CornerObject:
    def __init__(self):
        self.metadata = {"count": 3}


class UnsupportedCornerArguments:
    def __init__(self, value):
        self.value = value


def make_corner_payload():
    return {
        "metadata": {"count": 3},
        "values": [10, 20, 30],
        ("tuple", "key"): 7,
        "record": CornerRecord(4, 5),
    }


def make_corner_object():
    return CornerObject()


def check_shared_alias(left, right, *, keyword):
    assert left is right is keyword
    assert left == [3]
    return "shared-ok"


def check_cycle(value):
    assert value[0] is value
    assert value[1] == 3
    return "cycle-ok"


def check_dataclasses(mutable, frozen):
    assert isinstance(mutable, CornerArguments)
    assert mutable.value == 3
    assert mutable.nested == {"value": [20]}
    assert isinstance(frozen, FrozenCornerArguments)
    assert frozen.value == 7
    return "dataclass-ok"


def check_containers(value):
    assert value["list"] == [3, 20]
    assert value["tuple"] == (3, 20)
    assert value["set"] == {3, 20}
    assert value["frozenset"] == frozenset({3, 20})
    assert value["deque"] == deque([3, 20])
    assert value["namedtuple"] == CornerRecord(3, 20)
    return "containers-ok"


def write_corner_file(value):
    with open("shared-name.txt", "w") as stream:
        stream.write(value)
    return f"producer-{value}"


def read_corner_files(files, external, repeated=None):
    with open(files.left) as stream:
        left = stream.read()
    with open(files.nested["right"]) as stream:
        right = stream.read()
    with open(repeated) as stream:
        repeated_value = stream.read()
    with open(external) as stream:
        external_prefix = stream.read(15)
    assert left == repeated_value == "left"
    assert right == "right"
    assert "import argparse" in external_prefix
    assert files.left != files.nested["right"]
    return "files-ok"


def read_one_corner_file(path):
    with open(path) as stream:
        return stream.read()


def write_multiple_corner_files():
    os.makedirs("nested", exist_ok=True)
    Path("nested/first.txt").write_text("first")
    Path("second.txt").write_text("second")
    return "multiple-producer"


def read_multiple_corner_files(first, second):
    assert Path(first).read_text() == "first"
    assert Path(second).read_text() == "second"
    return "multiple-files-ok"


def verify_corner_results(*values, keyword_value=None):
    assert values == (
        3,
        20,
        7,
        4,
        3,
        "shared-ok",
        "cycle-ok",
        "dataclass-ok",
        "containers-ok",
        "files-ok",
        "right",
        "producer-left",
        "producer-right",
        "multiple-files-ok",
    )
    assert keyword_value == 30
    return "corner-cases-ok"


def make_corner_cases_graph():
    workflow = Workflow()
    payload = workflow.add_task(make_corner_payload)
    obj = workflow.add_task(make_corner_object)

    count = payload.output()["metadata"]["count"]
    second = payload.output()["values"][1]
    tuple_key = payload.output()[("tuple", "key")]
    namedtuple_index = payload.output()["record"][0]
    object_attribute = obj.output().attr("metadata")["count"]

    shared = [count]
    shared_check = workflow.add_task(check_shared_alias, shared, shared, keyword=shared)

    cyclic = []
    cyclic.append(cyclic)
    cyclic.append(count)
    cycle_check = workflow.add_task(check_cycle, cyclic)

    mutable_box = CornerArguments(count, {"value": [second]})
    frozen_box = FrozenCornerArguments(tuple_key)
    dataclass_check = workflow.add_task(check_dataclasses, mutable_box, frozen_box)

    container_check = workflow.add_task(
        check_containers,
        {
            "list": [count, second],
            "tuple": (count, second),
            "set": {count, second},
            "frozenset": frozenset({count, second}),
            "deque": deque([count, second]),
            "namedtuple": CornerRecord(count, second),
        },
    )

    external = workflow.file(__file__)
    left_producer = workflow.add_task(write_corner_file, "left")
    right_producer = workflow.add_task(write_corner_file, "right")
    left_file = left_producer.file("shared-name.txt")
    right_file = right_producer.file("shared-name.txt")
    file_consumer = workflow.add_task(
        read_corner_files,
        FileCornerArguments(left_file, {"right": right_file}),
        external,
        repeated=left_file,
    )
    second_consumer = workflow.add_task(read_one_corner_file, right_file)
    multiple_producer = workflow.add_task(write_multiple_corner_files)
    first_file = multiple_producer.file("nested/first.txt")
    second_file = multiple_producer.file("second.txt")
    multiple_consumer = workflow.add_task(read_multiple_corner_files, first_file, second_file)

    final = workflow.add_task(
        verify_corner_results,
        count,
        second,
        tuple_key,
        namedtuple_index,
        object_attribute,
        shared_check.output(),
        cycle_check.output(),
        dataclass_check.output(),
        container_check.output(),
        file_consumer.output(),
        second_consumer.output(),
        left_producer.output(),
        right_producer.output(),
        multiple_consumer.output(),
        keyword_value=payload.output()["values"][2],
    )
    assert isinstance(left_file, FileHandle)
    workflow._corner_target_id = final._task_id
    workflow._corner_file_target = left_file
    return workflow


def check_rejected_corner_cases():
    left = Workflow()
    right = Workflow()
    left_task = left.add_task(add, 1)
    assert isinstance(left_task.output(), TaskOutputHandle)

    try:
        left.add_task("legacy-key", add, 1)
    except TypeError as exc:
        assert "callable" in str(exc)
    else:
        raise AssertionError("legacy add_task(key, func, ...) was accepted")

    try:
        right.add_task(add, left_task.output())
    except ValueError as exc:
        assert "different Workflow" in str(exc)
    else:
        raise AssertionError("cross-workflow dependency was accepted")

    try:
        left.add_task(add, left_task)
    except TypeError as exc:
        assert "task.output()" in str(exc)
    else:
        raise AssertionError("bare TaskHandle argument was accepted")

    try:
        left.add_task(add, {left_task.output(): 1})
    except ValueError as exc:
        assert "dict key" in str(exc)
    else:
        raise AssertionError("TaskOutputHandle dictionary key was accepted")

    try:
        left.add_task(add, {("nested", left_task.output()): 1})
    except ValueError as exc:
        assert "dict key" in str(exc)
    else:
        raise AssertionError("nested TaskOutputHandle dictionary key was accepted")

    try:
        left.add_task(add, UnsupportedCornerArguments(left_task.output()))
    except TypeError as exc:
        assert "custom object" in str(exc)
    else:
        raise AssertionError("hidden dependency in arbitrary object was accepted")

    left_file = left.file(__file__)
    assert isinstance(left_file, FileHandle)

    try:
        right.add_task(add, left_file)
    except ValueError as exc:
        assert "different Workflow" in str(exc)
    else:
        raise AssertionError("cross-workflow FileHandle was accepted")

    try:
        left.add_task(add, {left_file: 1})
    except ValueError as exc:
        assert "dict key" in str(exc)
    else:
        raise AssertionError("FileHandle dictionary key was accepted")

    try:
        left.add_task(add, UnsupportedCornerArguments(left_file))
    except TypeError as exc:
        assert "custom object" in str(exc)
    else:
        raise AssertionError("hidden FileHandle was accepted")

    try:
        left.add_task(add, FileHandle(left._workflow_id, 999999))
    except ValueError as exc:
        assert "does not belong" in str(exc)
    else:
        raise AssertionError("unknown FileHandle was accepted")

    producer = left.add_task(add, 1)
    producer.file("same.txt")
    try:
        producer.file("same.txt")
    except ValueError as exc:
        assert "already declares" in str(exc)
    else:
        raise AssertionError("duplicate output path on one task was accepted")

    for invalid in ("", ".", "../escape", "/absolute"):
        try:
            producer.file(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid output path was accepted: {invalid!r}")

    assert not hasattr(producer, "produces")
    assert not hasattr(producer, "consumes")
    assert not hasattr(left, "declare_input_file")
    assert not hasattr(producer, "declare_output_file")
    assert not hasattr(left, "declare_file")
    assert not hasattr(producer, "declare_file")


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
    if name == "corner-cases":
        return make_corner_cases_graph()
    raise ValueError(name)


def parse_cases(specs):
    out = []
    for s in specs:
        name, _, n = s.strip().partition(":")
        out.append((name, None if not n else int(n)))
    return out


def _sink_tasks(workflow):
    return workflow.sink_tasks()


def _run_vine_graph(
    graph, n, task_group, port, port_file, logs, tag, out_dir, ckpt_dir,
    priority, manager_name, libcores,
):
    run_info = logs / tag
    if run_info.exists():
        shutil.rmtree(run_info)

    wf = build_graph(graph, n)
    corner_target = TaskHandle(wf, wf._corner_target_id) if graph == "corner-cases" else None
    targets = [corner_target, wf._corner_file_target] if corner_target is not None else _sink_tasks(wf)

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

    with VineGraph(port=port, name=manager_name, run_info_path=str(logs), run_info_template=tag) as m:
        if port_file:
            Path(port_file).write_text(str(m.port))

        m.set_params(
            {
                "checkpoint-dir": str(ckpt_dir),
                "extra-task-output-size-mb": [0.0, 0.0],
                "extra-task-sleep-time": [0.0, 0.0],
                "libcores": libcores,
                "output-dir": str(out_dir),
                "task-group": task_group,
                "task-priority-mode": priority,
                "wait-for-workers": 1,
            }
        )
        results = m.run(
            wf,
            targets=targets,
            hoisting_modules=[sys.modules[__name__]],
            env_files={"./vine_graph_workflow_examples.py": "vine_graph_workflow_examples.py"},
        ) or {}
        if graph == "corner-cases":
            assert results[corner_target] == "corner-cases-ok"
            target_path = results[wf._corner_file_target]
            assert Path(target_path).read_text() == "left"
        return {str(i): value for i, value in enumerate(results.values())}


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
    manager_name=None,
    libcores=4,
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
            return _run_vine_graph(
                graph,
                n,
                task_group,
                port,
                port_file,
                logs,
                tag,
                out_d,
                ckpt,
                priority,
                manager_name,
                libcores,
            )
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old)
    finally:
        if delete_root:
            shutil.rmtree(root, ignore_errors=True)


def main():
    check_rejected_corner_cases()
    p = argparse.ArgumentParser()
    p.add_argument("port_file", nargs="?")
    p.add_argument("-G", "--graph", nargs="+")
    p.add_argument("--case", action="append", dest="cases")
    p.add_argument("--task-group", type=int, default=0)
    p.add_argument("--task-priority-mode", default="random")
    p.add_argument("--port", type=int, default=0)
    p.add_argument("--manager-name")
    p.add_argument("--libcores", type=int, default=4)
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
                    manager_name=args.manager_name,
                    libcores=args.libcores,
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
