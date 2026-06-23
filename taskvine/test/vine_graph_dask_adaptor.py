import warnings

from ndcctools.taskvine.vine_graph.adaptors import VineGraphDaskAdaptor
from ndcctools.taskvine.vine_graph.workflow import TaskOutputRef


MIN_TASKS = 10


def require_module(name):
    """Import an optional test dependency, or skip that case if it is unavailable."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            module = __import__(name, fromlist=["*"])
    except Exception:
        return None
    return module


def get_delayed():
    """Return Dask's delayed constructor across common Dask versions."""
    try:
        from dask import delayed

        return delayed
    except ImportError:
        import dask

        return dask.delayed


def inc(value):
    return value + 1


def add_many(*values):
    return sum(values)


def assert_has_task_output_ref(value):
    """Return true if a converted argument tree contains a VineGraph dependency."""
    if isinstance(value, TaskOutputRef):
        return True
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(assert_has_task_output_ref(item) for item in value)
    if isinstance(value, dict):
        return any(assert_has_task_output_ref(item) for item in value.values())
    return False


def assert_converts(name, graph, min_tasks=1, expect_dependency=True):
    """Convert a Dask graph and check the basic VineGraph task-expression shape."""
    converted = VineGraphDaskAdaptor(graph).converted
    assert len(converted) >= min_tasks, f"{name}: expected at least {min_tasks} tasks, got {len(converted)}"

    has_dependency = False
    for _func, args, kwargs in converted.values():
        has_dependency = has_dependency or any(assert_has_task_output_ref(arg) for arg in args)
        has_dependency = has_dependency or any(assert_has_task_output_ref(value) for value in kwargs.values())

    if expect_dependency:
        assert has_dependency, f"{name}: expected at least one TaskOutputRef dependency"

    return converted


def check_low_level_dict():
    """Check Dask's explicit low-level task dictionary form."""
    graph = {f"x-{i}": (inc, i) for i in range(MIN_TASKS)}
    graph["total"] = (add_many, *graph.keys())

    assert_converts(
        "low-level-dict",
        graph,
        min_tasks=MIN_TASKS,
    )


def check_delayed():
    """Check the common dask.delayed graph construction API."""
    delayed = get_delayed()

    leaves = [delayed(inc, pure=True)(i) for i in range(MIN_TASKS)]
    total = delayed(add_many, pure=True)(*leaves)

    converted = assert_converts("delayed", {"result": total}, min_tasks=MIN_TASKS)

    add_many_tasks = [
        task_expr
        for task_expr in converted.values()
        if getattr(task_expr[0], "__name__", None) == "add_many"
    ]
    assert len(add_many_tasks) == 1

    _func, args, kwargs = add_many_tasks[0]
    assert kwargs == {}
    assert len(args) == MIN_TASKS
    assert all(isinstance(arg, TaskOutputRef) for arg in args)


def check_array_collection():
    """Check a Dask Array collection when the optional array package is available."""
    da = require_module("dask.array")
    if da is None:
        return

    values = da.arange(2 * MIN_TASKS, chunks=2)
    total = (values + 1).sum()
    assert_converts("array", {"result": total}, min_tasks=MIN_TASKS)


def check_bag_collection():
    """Check a Dask Bag collection when the optional bag package is available."""
    db = require_module("dask.bag")
    if db is None:
        return

    total = db.from_sequence(list(range(MIN_TASKS)), npartitions=MIN_TASKS).map(inc).sum()
    assert_converts("bag", {"result": total}, min_tasks=MIN_TASKS)


def check_dataframe_collection():
    """Check a Dask DataFrame collection when dataframe dependencies are available."""
    dd = require_module("dask.dataframe")
    pd = require_module("pandas")
    if dd is None or pd is None:
        return

    frame = pd.DataFrame({"value": list(range(MIN_TASKS))})
    ddf = dd.from_pandas(frame, npartitions=MIN_TASKS)
    total = (ddf.value + 1).sum()
    assert_converts("dataframe", {"result": total}, min_tasks=MIN_TASKS)


def main():
    check_low_level_dict()
    check_delayed()
    check_array_collection()
    check_bag_collection()
    check_dataframe_collection()


if __name__ == "__main__":
    main()
