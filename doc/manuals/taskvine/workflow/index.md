# Workflow Integration

## Parsl
TaskVine can be used as a workflow execution engine for Parsl workflows.
To install Parsl along with TaskVine, create a `conda` environment and
install `parsl` and `ndcctools` packages:

```sh
conda install ndcctools parsl
```
Using Parsl with TaskVine is as easy as loading the TaskVineExecutor
configuration and running the workflow as usual. For example,
below is a simple Parsl application executing a function remotely.

=== "Python"
    ```python
    import parsl
    from parsl import python_app
    from parsl.configs.vineex_local import config

    @python_app
    def double(x):
        return x*2

    with parsl.load(config=config) as dfk:
        future = double(1)
        assert future.result() == 2
    ```
Save this file as `parsl_vine_example.py`. Running
`python parsl_vine_example.py`
will automatically spawn a local worker to execute the function call.

In order to use the TaskVineExecutor with remote resources, you will need to create a configuration as shown below. Using the TaskVine Factory is the simplest way of deploying remote workers. Here a configuration for HTCondor is shown. It is necessary to include a `project_name` in the `TaskVineManagerConfig` in order for the Factory to find the manager.

=== "Python"
    ```python

    import parsl
    from parsl import python_app
    from parsl.config import Config
    from parsl.executors.taskvine import TaskVineExecutor
    from parsl.executors.taskvine import TaskVineFactoryConfig
    from parsl.executors.taskvine import TaskVineManagerConfig

    config = Config(
        executors=[
            TaskVineExecutor(
                factory_config=TaskVineFactoryConfig(
                    batch_type="condor",
                    min_workers=1,
                    max_workers=1,
                    cores=12,
                ),
                manager_config=TaskVineManagerConfig(
                    project_name="taskvine_parsl",
                )
            )
        ]
    )

    l = ["Cooperative", "Computing", "Lab"]

    @python_app
    def hello_taskvine(x, l=l):
        return l[x]

    with parsl.load(config=config) as dfk:
        futures = []
        for i in range(3):
            futures.append(hello_taskvine(i))

        for i in futures:
            print(i.result())
    ```

For more details on how to configure Parsl+TaskVine to scale applications
with compute resources of
local clusters and various performance optimizations, please refer to
the [Parsl documentation](https://parsl.readthedocs.io/en/stable/userguide/configuring.html).

## Dask

TaskVine can be used to execute Dask workflows using a manager as Dask
scheduler. The class `DaskVine` implements a TaskVine manager that has a
`get` that can be used as follows:

=== "Python"
    ```python
    import ndcctools.taskvine as vine
    import argparse
    import getpass
    import sys

    try:
        import dask
        import awkward as ak
        import dask_awkward as dak
        import numpy as np
    except ImportError:
        print("You need dask, awkward, and numpy installed")
        print("(e.g. conda install -c conda-forge dask dask-awkward numpy) to run this example.")

    behavior: dict = {}

    @ak.mixin_class(behavior)
    class Point:
        def distance(self, other):
	    return np.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)


    if __name__ == "__main__":
        # data arrays
        points1 = ak.Array([
	    [{"x": 1.0, "y": 1.1}, {"x": 2.0, "y": 2.2}, {"x": 3, "y": 3.3}],
	    [],
	    [{"x": 4.0, "y": 4.4}, {"x": 5.0, "y": 5.5}],
	    [{"x": 6.0, "y": 6.6}],
	    [{"x": 7.0, "y": 7.7}, {"x": 8.0, "y": 8.8}, {"x": 9, "y": 9.9}],
        ])

        points2 = ak.Array([
	    [{"x": 0.9, "y": 1.0}, {"x": 2.0, "y": 2.2}, {"x": 2.9, "y": 3.0}],
	    [],
	    [{"x": 3.9, "y": 4.0}, {"x": 5.0, "y": 5.5}],
	    [{"x": 5.9, "y": 6.0}],
	    [{"x": 6.9, "y": 7.0}, {"x": 8.0, "y": 8.8}, {"x": 8.9, "y": 9.0}],
        ])
        array1 = dak.from_awkward(points1, npartitions=3)
        array2 = dak.from_awkward(points2, npartitions=3)

        array1 = dak.with_name(array1, name="Point", behavior=behavior)
        array2 = dak.with_name(array2, name="Point", behavior=behavior)

        distance = array1.distance(array2)

        m = vine.DaskVine(port=9123, ssl=True)
        m.set_name("test_manager")
        print(f"Listening for workers at port: {m.port}")

        f = vine.Factory(manager=m)
        f.cores = 4
        f.max_workers = 1
        f.min_workers = 1
        with f:
            with dask.config.set(scheduler=m.get):
                result = distance.compute(resources={"cores": 1}, resources_mode="max", worker_transfers=True)
                print(f"distance = {result}")
            print("Terminating workers...", end="")
        print("done!")
    ```

The `compute` call above may receive the following keyword arguments:

| Keyword | Description |
|------------ |---------|
| environment | A TaskVine file (or a string path to a poncho env tarball) that provides an [environment](#execution-contexts) to execute each task. |
| env\_vars   | A dictionary of VAR=VALUE environment variables to set per task. A value should be either a string, or a function that accepts as arguments the manager and task, and that returns a string. |
| extra\_files | A dictionary of {taskvine.File: "remote_name"} of input files to attach to each task.|
| worker\_transfers | Whether to keep intermediate results only at workers for higher throughput (True, default), or to bring back each result to the manager for better fault tolerance (False). |
| resources   | A dictionary to specify [maximum resources](#task-resources), e.g. `{"cores": 1, "memory": 2000"}` |
| resources\_mode | [Automatic resource management](#automatic-resource-management) to use, e.g., "fixed", "max", or "max throughput"|
| task\_mode | Mode to execute individual tasks, such as [function calls](#serverless-computing). to use, e.g., "tasks", or "function-calls"|
| task\_priority\_mode | How tasks are prioritized for submission; higher priority is considered first (default "largest-input-first" for faster data pruning). |
| scheduling\_mode | Strategy to dispatch tasks to workers (default "files": prefer workers that already have more of the required input files). |
| retries | Number of times to attempt a task (default 5). |
| submit\_per\_cycle | Maximum number of tasks to submit to the scheduler per loop; None means no limit. |
| max\_pending | Maximum number of tasks without a result before new ones are submitted; None means no limit. |
| verbose | If true, emit additional debugging information. |
| progress\_disable | If True, disable progress bar. |
| progress\_label | Label to use in progress bar. |
| reconstruct | Reconstruct graph based on annotated functions. |
| merge\_size | When reconstructing a merge function, merge this many at a time. |
| wrapper | Function to wrap dask calls for debugging; should return `(wrapper result, dask call result)`. |
| wrapper\_proc | Function to process results from wrapper on completion (default is print). |
| prune\_depth | Control pruning behavior: 0 (default) no pruning; 1 checks direct consumers (most aggressive); 2+ checks consumers up to specified depth (the higher means more conservative). |
| env\_per\_task | If true, each task individually expands its own environment (requires `environment` be a string). |
| lib\_extra\_functions | Additional functions to include in execution library (only for `task_mode="function-calls"`). |
| lib\_resources | Resources for the execution library (only for `task_mode="function-calls"`), e.g. `{"cores": 4, "memory": 2000, "disk": 1000, "slots": 4}`. |
| lib\_command | Command to be prefixed to the execution of a Library task (only for `task_mode="function-calls"`). |
| lib\_modules | Hoist these execution-library imports to avoid redundant module imports across individual function invocations (only for `task_mode="function-calls"`). |
| lazy\_transfers | Deprecated alias for `worker_transfers`. |
| hoisting\_modules | Deprecated alias for `lib_modules`. |
| import\_modules | Deprecated alias for `lib_modules`. |


## Makeflow
Makeflow is capable of executing workflows using TaskVine workers,
let's look at an example of how this is done.

First, create a file called `example.makeflow` with the following contents:
```make
CURL=/usr/bin/curl
CONVERT=/usr/bin/convert
URL="https://ccl.cse.nd.edu/images/capitol.jpg"

capitol.anim.gif: capitol.jpg capitol.90.jpg capitol.180.jpg capitol.270.jpg capitol.360.jpg
    LOCAL $(CONVERT) -delay 10 -loop 0 capitol.jpg capitol.90.jpg capitol.180.jpg capitol.270.jpg capitol.360.jpg capitol.270.jpg capitol.180.jpg capitol.90.jpg capitol.anim.gif

capitol.90.jpg: capitol.jpg
    $(CONVERT) -swirl 90 capitol.jpg capitol.90.jpg

capitol.180.jpg: capitol.jpg
    $(CONVERT) -swirl 180 capitol.jpg capitol.180.jpg

capitol.270.jpg: capitol.jpg
    $(CONVERT) -swirl 270 capitol.jpg capitol.270.jpg

capitol.360.jpg: capitol.jpg
    $(CONVERT) -swirl 360 capitol.jpg capitol.360.jpg

capitol.jpg:
    LOCAL $(CURL) -o capitol.jpg $(URL)
```

Assuming you have installed `ndcctools` through conda,
launch the workflow in TaskVine mode:
```sh
makeflow -T vine example.makeflow
```
You can also specify a port by adding the `-p` option:
```
makeflow -T vine -p 9127 example.makeflow
```

Then while connected to your head node, submit 10 workers to Condor ensuring
that the port value matches:
```
vine_submit_workers -T condor $HOSTNAME 9127 10
```

Makeflow will send tasks to the workers until it completes. 
Once the workflow finishes, you can send more work to the workers,
or let them expire, which happens after 15 minutes of them being idle.

### Project names
Instead of a port you can use project names:
```
makeflow -T vine -N helloworm example.makeflow
```

Then submit 10 workers by using the selected name:
```
vine_submit_workers -T condor -M helloworm 10
```

