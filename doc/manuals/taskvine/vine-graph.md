# Vine Graph User Manual

Vine Graph is a Python interface for describing a directed acyclic graph (DAG)
of function calls and executing it with TaskVine. A `Workflow` records tasks and
their dependencies, while `VineGraph` manages execution and returns only the
results that the application requests.

This manual covers the current API on the `task-graph` branch. Every complete
example and command sequence in this document is exercised as part of the
manual's acceptance test.

## Build from source

From the root of the CCTools source tree:

```bash
./configure --prefix="$PWD/install"
make -j8
export PYTHONPATH="$PWD/test_support/python_modules/python3${PYTHONPATH:+:$PYTHONPATH}"
export PATH="$PWD/taskvine/src/worker:$PATH"
```

The build places the Vine Graph extension in the in-tree Python package. A
separate `make install` is not required for development-tree testing.

Vine Graph requires Python support, SWIG, and `cloudpickle`. The Dask adaptor
also requires Dask.

## The programming model

The main objects are:

- `Workflow`: owns a graph and all handles created for that graph.
- `TaskHandle`: identifies a task. `workflow.add_task()` returns one.
- `TaskOutputHandle`: represents a task's Python return value, or a selected
  part of that value. Obtain one with `task.output()`.
- `FileHandle`: represents an existing frontend file or a file produced in a
  task sandbox.
- `VineGraph`: a TaskVine manager that executes a completed workflow.

`VineGraph.run()` is synchronous: it returns after execution finishes. The
returned dictionary is keyed by the target handles supplied by the caller.

## First workflow: local execution

Local execution is useful for learning, debugging functions, and checking graph
construction without starting a worker. Save this as `vine_graph_local.py`:

```python
from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def make_record(value):
    return {"values": [value, value + 1], "metadata": {"count": 2}}


def scaled_sum(values, count, scale=1):
    assert len(values) == count
    return sum(values) * scale


workflow = Workflow()
record = workflow.add_task(make_record, 20)
answer = workflow.add_task(
    scaled_sum,
    record.output()["values"],
    record.output()["metadata"]["count"],
    scale=2,
)

with VineGraph(port=0) as manager:
    results = manager.run(
        workflow,
        targets=[answer],
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-local-output",
        },
    )

assert results[answer] == 82
print(results[answer])
```

Run it with:

```bash
python vine_graph_local.py
```

Passing `record` directly would be an error because a `TaskHandle` identifies a
task, not its value. Pass `record.output()` instead. Indexing an output handle,
as in `record.output()["values"]`, selects a value inside the consuming task
without adding another graph node.

To request every terminal result, pass `targets=workflow.sink_tasks()`.
`sink_tasks()` returns handles for all tasks with no downstream consumers.

## Passing files between tasks

Use `workflow.file(path)` for an existing frontend file. Use
`task.file(relative_path)` for a file that the task will create inside its
sandbox. A consumer receives either kind of handle as a local path string.

Save this as `vine_graph_files.py`:

```python
from pathlib import Path

from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def uppercase(source_path):
    text = Path(source_path).read_text().upper()
    Path("result.txt").write_text(text)
    return len(text)


def verify(result_path, expected_length):
    text = Path(result_path).read_text()
    assert len(text) == expected_length
    return text


Path("input.txt").write_text("vine graph\n")

workflow = Workflow()
source = workflow.file("input.txt")
producer = workflow.add_task(uppercase, source)
produced_file = producer.file("result.txt")
consumer = workflow.add_task(verify, produced_file, producer.output())

with VineGraph(port=0) as manager:
    results = manager.run(
        workflow,
        targets=[consumer, produced_file],
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-file-output",
        },
    )

assert results[consumer] == "VINE GRAPH\n"
assert Path(results[produced_file]).read_text() == "VINE GRAPH\n"
print(results[consumer], end="")
```

Task output paths must be non-empty relative paths that stay inside the task
sandbox. Each output path may be declared only once for a given task. Handles
belong to one `Workflow` and cannot be passed into another workflow.

## Distributed execution with a worker

Distributed mode is the default. The manager creates a TaskVine task-runner
library and waits for workers to execute graph nodes. Save this as
`vine_graph_distributed.py`:

```python
from pathlib import Path
import sys

from ndcctools.taskvine.vine_graph import VineGraph, Workflow


def square(value):
    return value * value


def add_all(*values):
    return sum(values)


port_file = Path(sys.argv[1])
workflow = Workflow()
squares = [workflow.add_task(square, value) for value in range(1, 6)]
total = workflow.add_task(add_all, *(task.output() for task in squares))

with VineGraph(port=0) as manager:
    port_file.write_text(str(manager.port))
    results = manager.run(
        workflow,
        targets=[total],
        params={
            "libcores": 4,
            "output-dir": "./vine-graph-distributed-output",
        },
        hoisting_modules=[sys.modules[__name__]],
        env_files={__file__: Path(__file__).name},
    )

assert results[total] == 55
print(results[total])
```

The following sequence starts the manager, waits for its port file, starts one
local worker, and waits for both processes to finish:

```bash
python vine_graph_distributed.py vine.port &
manager_pid=$!
while [ ! -s vine.port ]; do sleep 1; done
vine_worker --single-shot localhost "$(tr -d '[:space:]' < vine.port)"
wait "$manager_pid"
```

`hoisting_modules` makes module-level definitions available in the generated
task-runner library. Use it when task functions depend on classes, imported
modules, constants, or helper functions from the application module.
`env_files` maps frontend paths to names made available in the task-runner
environment.

To scale beyond one local worker, start workers with the usual TaskVine tools
for the target batch system and point them at the same manager address or name.

## Dask graphs

Set `from_dask=True` to convert a Dask-style graph. This supports low-level task
dictionaries and common Dask collection forms. Save this as
`vine_graph_dask.py`:

```python
from ndcctools.taskvine.vine_graph import VineGraph


def increment(value):
    return value + 1


def multiply(left, right):
    return left * right


dask_graph = {
    "incremented": (increment, 1),
    "answer": (multiply, "incremented", 10),
}

with VineGraph(port=0) as manager:
    results = manager.run(
        dask_graph,
        targets=["answer"],
        from_dask=True,
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-dask-output",
        },
    )

assert results["answer"] == 20
print(results["answer"])
```

For Dask collections, pass a dictionary of Dask collection objects. Do not mix
collection and non-collection values in that dictionary.

## Static Graphed plans

`VineGraphGraphedAdaptor` converts a static plan that provides `process`,
`combine`, `empty`, and `tasks`. Each task must have `key` and `partition`
attributes. Save this protocol example as `vine_graph_graphed.py`:

```python
from dataclasses import dataclass

from ndcctools.taskvine.vine_graph import VineGraph, VineGraphGraphedAdaptor


@dataclass
class PlanTask:
    key: str
    partition: tuple


class StaticPlan:
    next_tasks = None
    stop = None

    def __init__(self):
        self.tasks = [
            PlanTask("left", (1, 2)),
            PlanTask("right", (3, 4)),
        ]

    def empty(self):
        return 0

    def process(self, partition, resources):
        return sum(partition)

    def combine(self, left, right):
        return left + right


adapted = VineGraphGraphedAdaptor(StaticPlan())

with VineGraph(port=0) as manager:
    results = manager.run(
        adapted.converted,
        targets=adapted.targets,
        params={
            "local-execute": 1,
            "output-dir": "./vine-graph-graphed-output",
        },
    )

assert results[adapted.target] == 10
print(results[adapted.target])
```

Only static plans are supported. Plans with `next_tasks` or a stop condition are
rejected.

## Execution parameters

Parameters may be supplied through the `params` argument to `run()` or through
`manager.set_params()` before `run()`.

Useful parameters include:

| Parameter | Purpose |
| --- | --- |
| `local-execute` | Run in process when set to `1`; use TaskVine workers when `0`. |
| `output-dir` | Store serialized results; local mode also stores task sandboxes here. |
| `checkpoint-dir` | Store executor checkpoints here. |
| `libcores` | Number of cores assigned to the task-runner library. |
| `task-group` | Merge eligible linear chains when set to `1`. |
| `task-priority-mode` | Select graph scheduling order. |
| `progress-bar-update-interval-sec` | Control progress refresh frequency. |

Valid priority modes are `random`, `depth-first`, `breadth-first`, `fifo`,
`lifo`, `largest-input-first`, and `largest-storage-footprint-first`.

The `repeats` argument to `run()` can replicate a graph for throughput tests.
It cannot be combined with `FileHandle` dependencies.

## Supported argument structures

Task output and file handles can appear inside lists, tuples, dictionaries,
sets, frozen sets, deques, named tuples, and dataclass instances. Shared
references and cycles in mutable containers are preserved. Dependency handles
cannot be dictionary keys, and handles hidden inside arbitrary custom objects
are rejected; use a dataclass for structured arguments.

Task functions and arguments must be serializable by `cloudpickle`. A task must
create every file declared with `task.file()` before it exits successfully.

## Troubleshooting

- An import error for `_vine_graph_capi` usually means the source tree was not
  built after configuration, or `PYTHONPATH` does not point to the matching
  in-tree bindings.
- A manager that remains at zero completed tasks normally has no connected
  worker, or the worker cannot reach the manager port.
- A task-runner library failure often means a task function refers to a module,
  class, constant, or helper that was not included in `hoisting_modules`.
- Use a unique `output-dir` and `checkpoint-dir` for concurrent workflows.
- Use `local-execute` first to separate graph/function errors from worker or
  network errors.

## Run the project regression tests

From the repository root, run the enabled Vine Graph regression tests with:

```bash
cd taskvine/test
./TR_vine_graph_workflow_examples.sh prepare
./TR_vine_graph_workflow_examples.sh run
./TR_vine_graph_dask_adaptor.sh prepare
./TR_vine_graph_dask_adaptor.sh run
./TR_vine_graph_task_group.sh prepare
./TR_vine_graph_task_group.sh run
```

These tests cover distributed execution, structured arguments, frontend and
task-produced files, target selection, Dask conversion, task grouping, and
result equivalence.
