## Data Handling

### Caching and Sharing

Wherever possible, TaskVine retains files (whatever their sources) within
the cluster so that they can be reused by later tasks.  To do this
consistently, each file is given a **unique cache name** that is computed
from its contents and metadata.  This ensures that if the external source
for a file changes, any old cached copies will not be reused.  In addition,
cached files used concurrently by multiple tasks may be transferred
between workers to share them efficiently.

If necessary, you can control the caching behavior of files individually.

- A cache value of **task** indicates that the file should be deleted as
soon as it is consumed by a task.  This is appropriate for input files
that are specific to one task, and one task only.
- A cache value of **workflow** (the default) indicates that the file
should be retained as long as the workflow runs, and then deleted at the end.
- A cache value of **worker** indicates that the file should be retained
by the worker until the worker's end-of-life.
- A cache value of **forever** indicates that the file should be retained
by the worker, even across workflows.  This is appropriate for widely used
software packages and reference datasets. This level of cache leaves files on
the execution sites even when workers terminate, thus use with care.

=== "Python"
    ```python
    f = m.declare_file("myfile.txt", cache="task")       # (default, same as cache=False)
    f = m.declare_file("myfile.txt", cache="workflow")   # (same as cache=True)
    f = m.declare_file("myfile.txt", cache="worker")
    f = m.declare_file("myfile.txt", cache="forever")
    ```
=== "C"
    ```
    vine_declare_file(m, "myfile.txt", VINE_CACHE_LEVEL_TASK, 0)
    vine_declare_file(m, "myfile.txt", VINE_CACHE_LEVEL_WORKFLOW, 0)
    vine_declare_file(m, "myfile.txt", VINE_CACHE_LEVEL_WORKER, 0)
    vine_declare_file(m, "myfile.txt", VINE_CACHE_LEVEL_FOREVER, 0)
    ```

TaskVine generally assumes that a file created on one worker can always
be transferred to another.  It is occasionally the case that a file created
on a specific worker is truly specialized to that machine and should
not be transferred.  (For example, if a MiniTask compiled some code specifically for the architecture of a given machine.)  In that case, you should indicate
that peer transfers are not permitted:

=== "Python"
    ```python
    f = m.declare_file("myfile.txt", cache="task", peer_transfer=False)
    ```
=== "C"
    ```
    vine_declare_file(m, "myfile.txt", VINE_CACHE_LEVEL_WORKFLOW, VINE_PEER_NOSHARE)
    ```
Automatic sharing of files between workers, or peer transfers, are enabled by default
in TaskVine. If communication between workers is not possible or not desired, peer transfers
may be globally disabled:

=== "Python"
    ```python
    m.disable_peer_transfers()
    ```
=== "C"
    ```
    vine_disable_peer_transfers(m);
    ```
If peer transfers have been disabled, they may be re-enabled accordingly:

=== "Python"
    ```python
    m.enable_peer_transfers()
    ```
=== "C"
    ```
    vine_enable_peer_transfers(m);
    ```

Transfers between workers may be impacted by transient issues which may cause intermittent transfer failures. In these situations we take note of the
failure that occurred, and avoid using the same worker as a source for a period of time. This time period has a default value of 15 seconds.
It may be changed by the user using `vine_tune` with the parameter `transient-error-interval`.

### MiniTasks

A task can be used to perform custom fetch operations for input data. TaskVine
calls these tasks **mini tasks**, and they are defined in the same way as
regular tasks. Their only differences are that they are not submitted directly
to the manager, and that their output (either a file or a directory) has to be
specially declared.

This gives a lot of flexibility, as say for example, say you would like to
expand a compressed file that TaskVine does not natively support, or you would
like the input to be the result of a query to a database.

=== "Python"
    ```python
    # use cpio to expand archives coming from a url
    t = Task("cpio -iD output_dir < archive.cpio")

    my_url = m.declare_url("http://somewhere.com/archive.cpio", cache="workflow")
    t.add_input(my_url, "archive.cpio")

    mini_task = m.declare_mini_task(t, "output_dir")

    # regular tasks can use the mini task as input # the output of the mini
    # task is mounted in the regular task sandbox

    my_other_task = Task("my_cmd output_from_cpio/")
    my_other_task.add_input(mini_task, "output_from_cpio")

    # we submit to the manager only the regular task
    m.submit(my_other_task)
    ```


=== "C"
    ```C
    // use cpio to expand archives coming from a url
    struct vine_task *t = vine_task_create("cpio -iD output_dir < archive.cpio")

    struct vine_file *my_url = vine_declare_url("http://somewhere.com/archive.cpio", VINE_CACHE_LEVEL_WORKFLOW, 0);
    vine_task_add_input(my_url, "archive.cpio", 0);

    struct vine_file *mini_task = m.declare_mini_task(t, "output_dir")

    // regular tasks can use the mini task as input
    // the output of the mini task is mounted in the regular task sandbox

    struct vine_task *my_other_task = vine_task_create("my_cmd output_from_cpio/");
    vine_task_add_input(mini_task, "output_from_cpio");

    // we submit to the manager only the regular task
    vine_submit(m, my_other_task);
    ```

### Execution Contexts

The execution of a task can be wrapped with specially designed packages called execution contexts.
These ensure that the software dependencies for the
task are available in the execution site. TaskVine natively supports two types
of environments: [poncho](../poncho/index.md), which is based on `conda-pack`;
and [starch](../man_pages/starch.md), a lightweight package useful when the
manager and workers run the same linux version. Mini tasks can be used to
create environments not natively supported, as we will show later to construct
execution contexts for Apptainer (i.e., singularity containers).

#### Poncho

A Poncho package is a tarball based on `conda-pack`, and is useful to deliver
a complete python execution context. For example, to create a python package containing `numpy`:

`my_poncho_spec.json`
```json
{
    "conda": {
        "channels": [
            "conda-forge"
        ],
        "dependencies": [
            "python=3.10",
            "numpy=1.24.2"
        ]
    }
}
```

From the command line, create the poncho package like this:

```sh
poncho_package_create my_poncho_spec.json my_poncho_pkg.tar.gz
```

Attach the package to the task:

=== "Python"
    ```python
    # my task that requires python and numpy
    t = Task("python my_numpy_script.py")

    s = m.declare_file("my_numpy_script.py", cache="workflow")
    t.add_input(s, "my_numpy_script.py")

    # declare the package and its input file
    poncho_file = m.declare_file("my_poncho_pkg.tar.gz", cache="workflow")
    poncho_pkg = m.declare_poncho(poncho_file, cache="workflow")

    # attach the package to the task
    t.add_poncho_package(poncho_pkg)

    m.submit(t)
    ```

=== "C"
    ```C
    // my task that requires python and numpy
    struct vine_task *t = vine_task_create("python my_numpy_script.py");

    struct vine_file *s = vine_declare_file("my_numpy_script.py", VINE_CACHE_LEVEL_WORKFLOW, 0);
    vine_task_add_input(t, "my_numpy_script.py", 0);

    // declare the package and its input file
    struct vine_file *poncho_file = vine_declare_file("my_poncho_pkg.tar.gz", cache="workflow");
    struct vine_file *poncho_pkg  = vine_declare_poncho(poncho_file, cache="workflow")

    # attach the package to the task
    vine_task_add_poncho_package(t, poncho_pkg);

    vine_submit(m, t);
    ```

#### Starch

(to do)

#### Custom Execution Contents

TaskVine expects execution contents to expand to a directory, with this minimal
structure:

```text
root
└── bin
    └── run_in_env
```

where `run_in_env` is an executable file (usually a shell script) that takes as
an argument a command line to execute. In the rest of this section we will show
how to construct an execution context that runs its command line inside an Apptainer
container.

##### Apptainer Execution Context

Our script `run_in_env` script simply calls Apptainer with the desired image, and
mounts the task's sandbox as the home directory:

**run_command_in_apptainer.sh**
```shell
#! /bin/sh

# Wrap tasks with an Apptainer container

# get the directory that contains the execution context from the location of this script
ctx_dir=$(dirname $( cd -- "$( dirname -- "$0" )" > /dev/null 2>&1 && pwd ))

# execute the command line with the container image "image.img"
exec apptainer exec --home "${VINE_SANDBOX:-${PWD}}" "${ctx_dir}/image.sif" "$@"

```

To start, we can manually construct in the command line the needed directory
structure as follows. Later we will automate these steps with a mini task.

```sh
# ensure the right execution permissions for the script
chmod 755 run_command_in_apptainer.sh

# construct the needed directory structure
mkdir -p my_ctx/bin

# copy the apptainer script to the expected run_in_env location
cp run_command_in_apptainer.sh my_ctx/bin/run_in_env

# copy the desired image into the package
cp path/to/my_image.img my_ctx/image.img
```

Now we are ready to declare the execution context from its local directory "my_ctx":

=== "Python"
    ```python
    t = Task("/bin/echo from inside apptainer!")

    ctx = m.declare_file("my_ctx", cache="workflow")
    t.add_execution_context(ctx)

    m.submit(t)
    ```

=== "C"
    ```C
    struct vine_task *t = vine_task_create("/bin/echo from inside apptainer!");

    struct vine_file *ctx = vine_declare_file(m, "my_ctx", VINE_CACHE_LEVEL_WORKFLOW, 0);
    vine_task_add_execution_context(ctx);

    vine_submit(t);
    ```


##### Apptainer Execution Context From a Mini Task

In the previous section we manually built the directory structure needed for
the execution context. This is not very flexible, as we need to create one such
directory per container image that we would like to use. Instead, we can use a
mini task to construct the execution context directly on the workers.


=== "Python"
    ```python
    # construct the mini task. We only need the mini task for its sandbox to
    # create the environment structure, thus we use the command ":" as no-op.
    mt = Task(":")

    runner = m.declare_file("run_command_in_apptainer.sh", cache="workflow")
    image  = m.declare_file("path/to/my_image.img", cache="workflow")

    mt.add_input(runner, "ctx/bin/run_in_env")
    mt.add_input(image,  "ctx/image.img")

    # the mini task will extract the environment directory
    ctx = m.declare_mini_task(mt, "ctx")

    # now we define our regular task, and attach the environment to it.
    t = Task("/bin/echo from inside apptainer!")
    t.add_execution_context(ctx)

    m.submit(t)
    ```

You can see the complete example [here](examples/vine_example_apptainer_ctx.py).

### Watching Output Files

If you would like to see the output of a task as it is produced, add
the watch flag as an argument of `add_file`. This will
cause the worker to periodically send output appended to that file back to the
manager. This is useful for a program that produces a log or progress bar as
part of its output.

=== "Python"
    ```python
    t.add_output_file("my-file", watch=True)
    ```

=== "C"
    ```C
    vine_task_add_file(t, "my-file", "my-file", VINE_OUTPUT, VINE_WATCH);
    ```

### Optional Output Files

It is sometimes useful to return an output file only in the case of a failed task.
For example, if your task generates a very large debugging output file `debug.out`,
then you might not want to keep the file if the task succeeded.  In this case,
you can mark the file as a "failure-only" output to indicate that it should
only be returned when the task fails:

=== "Python"
    ```python
    my_debug = m.declare_file("debug.out", cache="task")
    t.add_output(my_debug, "debug.out", failure_only=True)
    ```

=== "C"
    ```C
    struct vine_file *my_debug = vine_declare_file("debug.out", VINE_CACHE_LEVEL_TASK, 0);
    vine_task_add_output(t, "debug.out", VINE_FAILURE_ONLY);
    ```

In a similar way, files can be marked to indicate that they should be returned on success:

=== "Python"
    ```python
    my_debug = m.declare_file("debug.out", cache="task")
    t.add_output(my_debug, "debug.out", success_only=True)
    ```

=== "C"
    ```C
    struct vine_file *my_debug = vine_declare_file("debug.out", VINE_CACHE_LEVEL_TASK, 0);
    vine_task_add_output(t, "debug.out", VINE_SUCCESS_ONLY);
    ```
