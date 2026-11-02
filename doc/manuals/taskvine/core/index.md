## Writing a TaskVine Application

A TaskVine application can be written in Python, or C.
In each language, the underlying principles are the same, but there are some syntactic differences shown below.
The full API documentation for each language is here:

- [TaskVine Python API](../api/html/namespacendcctools_1_1taskvine.html)
- [TaskVine C API](../api/html/taskvine_8h.html)

### Creating a Manager Object

To begin, you must import the TaskVine module, and then create a Manager object.
You may specify a specific port number to listen on like this:

=== "Python"
    ```python
    # Import the taskvine module
    import ndcctools.taskvine as vine

    # Create a new manager listening on port 9123
    m = vine.Manager(9123)
    ```

=== "C"
    ```
    /* Import the taskvine module */
    #include "taskvine.h"

    /* Create a new manager listening on port 9123 */
    struct vine_manager *m = vine_create(9123);
    ```

In a shared environment, that specific port might already be in use, and so you may find it more convenient
to specify zero to indicated any available port:

=== "Python"
    ```python
    # Create a new manager listening on any port
    m = vine.Manager(0)
    print(f"listening on port {m.port}")
    ```

=== "C"
    ```
    /* Create a new manager listening on any port */
    struct taskvine *m = vine_create(0);
    printf("listening on port %d\n", vine_port(m));

    ```

### Declaring Files

Each file used in a TaskVine application must be declared to the manager,
so that it can be cached and replicated within the cluster as needed.
The following are examples of basic file descriptions:

=== "Python"
    ```python
    a = m.declare_file("mydata.txt")
    d = m.declare_file("dataset/")
    u = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/db/human_genome.00.tar.gz")
    b = m.declare_buffer("These words are the contents of the file.")
    t = m.declare_temp()
    ```
=== "C"
    ```
    struct vine_file *a = vine_declare_file(m, "mydata.txt", VINE_CACHE_LEVEL_WORKFLOW, 0);
    struct vine_file *d = vine_declare_file(m, "dataset/", VINE_CACHE_LEVEL_WORKFLOW, 0);
    struct vine_file *u = vine_declare_url(m, "https://ftp.ncbi.nlm.nih.gov/blast/db/human_genome.00.tar.gz", VINE_CACHE_LEVEL_WORKFLOW, 0);
    struct vine_file *b = vine_declare_buffer(m, "These words are the contents of the file.", VINE_CACHE_LEVEL_WORKFLOW, 0);
    struct vine_file *t = vine_declare_temp(m);
    ```

`declare_file` indicates a file in the manager's local filesystem
which will be transferred into the cluster and made available to tasks.
Both files and directories can be declared in this way:
`declare_file("mydata.txt")` indicates a single text file,
while `declare_file("dataset")` refers to an entire directory tree.
A local file or directory can also be used as the output of a task.

`declare_url` indicates a remote dataset that will be loaded
as needed into the cluster.  This URL can be `http`, `https`,
`ftp`, `file` or any other method supported by the standard `curl` tool.
If many tasks need the same remote
URL, TaskVine is careful to limit the number of concurrent transfers,
and will seek to duplicate the file within the cluster, rather than
subject the source to a denial-of-service attack.

`declare_buffer` indicates a literal string of data that will be
presented to that task in the form of a file.  A buffer with no
initial data can also be used as the output of a task, made easily
visible within the manager application.

`declare_temp` indicates an unnamed ephemeral file that can be
used to capture the output of a task, and then serve as the input
of a later task.  Temporary files exist only within the cluster
for the duration of a workflow, and are deleted when no longer needed.
This accelerates a workflow by avoiding the step of returning the
data to the manager.
If a temporary file is unexpectedly lost due to the crash or failure
of a worker, then the task that created it will be re-executed. Temp files
may also be replicated across workers to a degree set by the `vine_tune` parameter
`temp-replica-count`. Temp file replicas are useful if significant work
is required to re-execute the task that created it.
The contents of a temporary file can be obtained with `fetch_file`

If it is necessary to unpack a file before it is used,
use the `declare_untar` transformation to wrap the file definition.
This will permit the unpacked version to be shared by multiple
tasks at once:

=== "Python"
    ```python
    u = m.declare_url("https://ftp.ncbi.nlm.nih.gov/blast/db/human_genome.00.tar.gz")
    x = m.declare_untar(u)
    ```

=== "C"
    ```C
    struct vine_file *u = vine_declare_url(m, "https://ftp.ncbi.nlm.nih.gov/blast/db/human_genome.00.tar.gz", VINE_CACHE_LEVEL_WORKFLOW, 0);
    struct vine_file *x = vine_declare_untar(m, u);
    ```

`declare_untar` is an example of a [MiniTask](#minitasks), which is explained further below.


### Declaring Tasks

TaskVine supports several forms of tasks: **Standard Tasks** consist
of Unix command lines, **Python Tasks** consist of Python functions
and their dependencies, and **Serverless Tasks** consist of invocations
of functions in remote libraries.

A **Standard Task** consists of a Unix command line to execute
and the resources needed for that task.  Previously declared input
and output files must be attached to the task to provide it with
the necessary data.

Here is an example of a task that consists of the standard Unix `gzip` program,
which will read the file `mydata` and produce `mydata.gz` as an output:

=== "Python"
    ```python
    t = vine.Task("gzip < mydata > mydata.gz")
    t.add_input(a, "mydata")
    t.add_output(b, "mydata.gz")
    ```

=== "C"
    ```C
    struct vine_task *t = vine_task_create("gzip < mydata > mydata.gz");
    vine_task_add_input(t, a, "mydata", 0)
    vine_task_add_output(t, b, "mydata.gz", 0)
    ```

Note that each task will execute in a private sandbox at a worker.
And so, each input and output file must be "attached" to the task
under a specific name.  The task will only have access to those
files specifically declared, and should not assume access to
a general shared filesystem.

When the task executes, the worker will create a **sandbox** directory,
which serves as the working directory for the task.  Each of the input files
and directories will be linked into the sandbox directory with the given remote names.
The task should write its outputs into the current working directory,
which will be extracted from the locations given in the `add_output` statements.

The path of the sandbox directory is exported to
the execution environment of each worker through the `VINE_SANDBOX` shell
environment variable. This shell variable can be used in the execution
environment of the worker to describe and access the locations of files in the
sandbox directory.

!!! warning
    The remote names given go to the files should match the names in the
    command line of the task.

In Python you may find it more convenient to declare a task as a dictionary.

=== "Python"
    ```python
    t = vine.Task(
        command = "./gzip < mydata > mydata.gz",
        input_files = {
            a : {
                remote_name : "mydata",
                cache : False
            }
        },
        output_files = {
            b : {
                remote_name : "mydata.gz",
                cache : False
            }
        }
    )
    ```

In addition to describing the input and output files, you may optionally
specify additional details about the task that will assist TaskVine in
making good scheduling decisions.

If you are able, describe the resources needed by each task (cores, gpus,
memory, disk) so that the worker can pack as many concurrent tasks.
This is described in greater detail under [Managing Resources](#managing-resources).

You may also attach a `tag` to a task, which is just a user-defined string
that describes the purpose of the task.  The tag is available as `t.tag`
when the task is complete.

=== "Python"
    ```python
    t.set_cores(2)
    t.set_memory(4096)
    t.set_tag("config-4.5.0")

    # this can once again be done at task declaration as well:
     t = vine.Task(
        command = "./gzip < my-file > my-file.gz",
        cores = 2,
        memory = 4096,
        tag = "config-4.5.0"
     )
    ```

=== "C"
    ```C
    vine_task_set_cores(t, 2);
    vine_task_set_memory(t, 4096);
    vine_task_set_tag(t, "config-4.5.0");
    ```

### Managing Tasks

Once a task has been fully specified, it can be submitted to the manager.
`submit` returns a unique taskid that can be helpful when later referring
to a task:

=== "Python"
    ```python
    taskid = m.submit(t)
    ```

=== "C"
    ```C
    int taskid = vine_submit(m, t);
    ```

Once all tasks are submitted, use `wait` to wait until a task completes,
indicating how many seconds you are willing to pause.  If a task completes
within that time limit, then `wait` will return that task object.
If no task completes within the timeout, it returns null.

=== "Python"
    ```python
    while not m.empty():
        t = m.wait(5)
        if t:
            print(f"Task {t.id} has returned!")

            if t.successful():
                print(f"stdout:\n{t.std_output}")
            if t.completed():
                print(f"task complete with error exit code: {t.exit_code}")
            else:
                print(f"There was a problem executing the task: {t.result}")
    ```

=== "C"
    ```C
    while(!vine_empty(m)) {
        struct vine_task *t = vine_wait(m, 5);
        if(t) {
            printf("Task %d has returned!\n", t->taskid);
            int result = vine_task_get_result(t);
            if(result == VINE_RESULT_SUCCESS) {
                int exit_code = vine_task_get_exit_code(t);
                if(exit_code == 0) {
                    printf("stdout: %s\n", vine_task_get_stdout(t));
                } else {
                    printf("task complete with error exit code: %d\n", exit_code);
                } else {
                    printf("There was a problem executing the task: %s\n", vine_result_string(result));
                }
            }
        }
    }
    ```

A completed task will have its output files written to disk.
You may examine the standard output of the task in `output` and the exit code in `exit_status`.

!!! note
    The size of standard output is limited to 1 GB. Any output beyond 1 GB will be
    truncated.  If large output is expected, redirect the stdout `./my-command > my-stdout` of the
    task to a file and specify the file as an output file of the task as
    described above.

When you are done with the task, delete it (only needed for C):

=== "C"
    ```C
    vine_task_delete(t);
    ```

Continue submitting and waiting for tasks until all work is complete. You may
check to make sure that the manager is empty with `vine_empty`. When all
is done, delete the manager (only needed for C):

=== "C"
    ```C
    vine_delete(m);
    ```

Full details of all of the TaskVine functions can be found in the [TaskVine API](../api/html/taskvine_8h.html).

## Advanced Task Handling

A variety of advanced features are available for programs with unusual needs
or very large scales. Each feature is described briefly here, and more details
may be found in the [TaskVine
API](../api/html/taskvine_8h.html).

### Security

By default, TaskVine does **not** perform any encryption or authentication,
so any workers will be able to connect to your manager, and vice versa. This
may be fine for a short running anonymous application, but is not safe for a
long running application with a public name.

We recommend that, at a minimum, you enable an application password to provide
authentication between managers and workers.  And, consider enabling SSL
to provide communication encryption.
These features can be enabled independently.

#### Password Authentication

We recommend that you enable a password for your TaskVine applications.
Create a file `vine.password` that contains a long string of random data like this:

```
openssl rand -hex 32 > vine.password
```

This password will be particular to your application, and only managers and
workers with the same password will be able to interoperate.
Then, modify your manager program to use the password:

=== "Python"
    ```python
    m.set_password_file("vine.password")
    ```

=== "C"
    ```C
    vine_set_password_file(m, "vine.password");
    ```


And give the `--password` option to give the same password file to your
workers:

```sh
$ vine_worker --password vine.password -M myproject
```

With this option enabled, both the manager and the workers will verify that the
other has the matching password before proceeding.  Likewise, when workers perform
peer-to-peer transfers, the password will be verified.

Note that the password is **not** sent in the clear, but is securely verified
through a SHA1-based mutual challenge-response protocol.

#### SSL Encryption

TaskVine can encrypt the communication between manager and workers using SSL.
For this, you need to set the key and certificate (in PEM format) of your
server when creating the manager.

If you do not have a key and certificate at hand, but you want the
communications to be encrypted, you can create your own key and certificate:

```sh
# Be aware that since this certificate would not be signed by any authority, it
# cannot be used to prove the identity of the server running the manager.

openssl req -x509 -newkey rsa:4096 -keyout MY_KEY.pem -out MY_CERT.pem -sha256 -days 365 -nodes
```

To activate SSL encryption, indicate the paths to the key and certificate when
creating the manager:

=== "Python"
    ```python
    # Import the taskvine module
    import ndcctools.taskvine as vine
    m = vine.Manager(port=9123, ssl=('MY_KEY.pem', 'MY_CERT.pem'))

    # Alternatively, you can set ssl=True and let the python API generate
    # temporary ssl credentials for the manager:
    m = vine.Manager(port=9123, ssl=True)
    ```

=== "C"
    ```
    /* Import the taskvine module */
    #include "taskvine.h"

    /* Create a new manager listening on port 9123 */
    struct vine_manager *m = vine_ssl_create(9123, 'MY_KEY.pem', 'MY_CERT.pem');
    ```


If you are using a [project name](#project-names-and-the-catalog-server) for
your manager, then the workers will be aware that the manager is using SSL and
communicate accordingly automatically. However, you are directly specifying the
address of the manager when launching the workers, then you need to add the
`--ssl` flag to the command line, as:

```sh
vine_worker (... other args ...) --ssl HOST PORT
vine_factory (... other args ...) --ssl HOST PORT
vine_status --ssl HOST PORT
vine_submit_workers -T condor -E'--ssl' HOST PORT
```

### Maximum Retries

When a task cannot be completed because a worker disconnects or because it
exhausted some intermediate resource allocation, it is automatically retried.
By default, there is no limit on the number of retries. However, you can set a
limit on the number of retries:

=== "Python"
    ```python
    t.set_retries(5)   # Task will be try at most 6 times (5 retries).

    # this can be done at task declaration as well:
     t = vine.Task(
        command = ...,
        retries = 5
     )
    ```

=== "C"
    ```C
    vine_set_retries(t, 5)
    ```

When a task cannot be completed in the set number of tries, then the its result
is set to the result of the last attempt (e.g. `"resource exhaustion"` in python,
or `VINE_RESULT_RESOURCE_EXHAUSTION` in C).


### Pipelined Submission

If you have a **very** large number of tasks to run, it may not be possible to
submit all of the tasks, and then wait for all of them. Instead, submit a
small number of tasks, then alternate waiting and submitting to keep a constant
number in the manager. The `hungry` will tell you if more submissions are
warranted:

=== "Python"
    ```python
    if m.hungry():
        # submit more tasks...
    ```

=== "C"
    ```C
    if(vine_hungry(m)) {
        // submit more tasks...
    }
    ```

### Task Result Caching

For workflows where the same task is submitted multiple times with identical arguments (such as during iterative development or notebook re-execution), TaskVine can cache task outputs and return them without dispatching to a worker. Enable caching by calling `enable_tasks_cache` on the manager:

```python
m = vine.Manager(port=9123)
m.enable_tasks_cache(cache_dir="vine-cache-outputs", log_file="vine-cache.txlog")
```

When a task completes, its output is stored in `cache_dir` and indexed by a fingerprint of the function and its arguments. On subsequent submissions of an identical task, the result is returned directly from cache. The cache persists across manager restarts via the transaction log, so results from a previous run are available immediately in a new session. Tasks that do not match any cached result are executed normally on a worker.

### Automatic Garbage Collection on Disk

For workflows that generate partial results that are not needed once a final
result has been computed, TaskVine can automatically delete them from disk when
the application indicates that they will not be needed anymore:

=== "Python"
    ```python
    partial_result = m.declare_file("my_partial_result", unlink_when_done=True)

    t1 = Task(...)
    t1.add_output(partial_result, "my_partial_result")
    ...

    t2 = Task(...)
    t2.add_input(partial_result, "my_partial_result")
    ...

    # once t2 is done, the following call will remove the file from the
    # taskvine workflow. Further, when no task refers to the file, the file
    # will be removed from the manager's disk because of unlink_when_done=True
    # at its declaration.
    m.undeclare_file(partial_result)
    ```

=== "C"
    ```C
    struct vine_file *partial_result = vine_declare_file(m, "my_partial_result", VINE_UNLINK_WHEN_DONE);

    struct vine_task *t1 = vine_task_create(...);
    vine_task_add_output(partial_result, "my_partial_result", /* any desired mount flags */ 0);
    ...

    struct vine_task *t2 = vine_task_create(...);
    vine_task_add_input(partial_result, "my_partial_result", /* any desired mount flags */ 0);
    ...

    # once t2 is done and deleted with `vine_task_delete`, the following call
    # will remove the file from the taskvine workflow. Further, when no task
    # refers to the file, the file will be removed from the manager's disk
    # because of VINE_UNLINK_WHEN_DONE at its declaration.
    vine_undeclare_file(partial_result);
    ```

!!! warning
    Never use this feature on files that the TaskVine application did not create. Otherwise you
    run the risk of removing irreplaceable input files



### Disconnect slow workers

A large computation can often be slowed down by stragglers. If you have a
large number of small tasks that take a short amount of time, then
automatically disconnecting slow workers can help. With this feature enabled,
statistics are kept on tasks execution times and statistical outliers are
terminated. If two different tasks are canceled in the same worker, then the
worker is disconnected and blacklisted.

=== "Python"
    ```python
    # Disconnect workers that are executing tasks twice as slow as compared to the average.
    m.enable_disconnect_slow_workers(2)
    ```

=== "C"
    ```C
    // Disconnect workers that are executing tasks twice as slow as compared to the average.
    vine_enable_disconnect_slow_workers(m, 2);
    ```

Tasks terminated this way are automatically retried in some other worker.
Each retry allows the task to run for longer times until a
completion is reached. You can set an upper bound in the number of retries with
[Maximum Retries](#maximum-retries).


### String Interpolation

If you have workers distributed across multiple operating systems (such as
Linux, Cygwin, Solaris) and/or architectures (such as i686, x86_64) and have
files specific to each of these systems, this feature will help. The strings
$OS and $ARCH are available for use in the specification of input file names.
TaskVine will automatically resolve these strings to the operating system
and architecture of each connected worker and transfer the input file
corresponding to the resolved file name. For example:

=== "Python"
    ```python
    my_exec = m.declare_file("my-executable.$OS.$ARCH",  cache="workflow")
    t.add_input_input(my_exec, "my_exe")
    ```

=== "C"
    ```C
    struct vine_file *my_exec = vine_declare_file(m, "my-executable.$OS.$ARCH",  VINE_CACHE_LEVEL_WORKFLOW, 0);
    add_input_input(my_exec, "my_exe", 0);
    ```

This will transfer `my-executable.Linux.x86_64` to workers running on a Linux
system with an x86_64 architecture and `a.Cygwin.i686` to workers on Cygwin
with an i686 architecture. These files will be named "my_exe" in the task's
sandbox, which means that the command line of the tasks does not need to
change.

Note this feature is specifically designed for specifying and distinguishing
input file names for different platforms and architectures. Also, this is
different from the $VINE_SANDBOX shell environment variable that exports
the location of the working directory of the worker to its execution
environment.


### Task Cancellations

This feature is useful in workflows where there are redundant tasks or tasks
that become obsolete as other tasks finish.  Tasks can be removed either
by either `task_id` or `tag`.  Tasks removed in this way will still be
returned in the usual way via `wait` with a `result` of `VINE_RESULT_CANCELLED`.
For example:

=== "Python"
    ```python
    # create task as usual and tag it with an arbitrary string.
    t = vine.Task(...)
    t.set_tag("my-tag")

    # or set tag in task declaration
    t = vine.Task(
        command = ...,
        tag = "my-tag"
    )

    taskid = m.submit(t)

    # cancel task by id.
    m.cancel_by_taskid(taskid)

    # or cancel task by tag.
    m.cancel_by_tasktag("my-tag")
    ```

=== "C"
    ```C
    // create task as usual and tag it with an arbitrary string.
    struct vine_task *t = vine_task_create("...");
    vine_set_task(t, "my-tag");

    int taskid = vine_submit(m, t);

    // cancel task by id.
    vine_cancel_by_taskid(m, taskid);

    # or cancel task by tag.
    vine_cancel_by_tasktag(m, "my-tag");
    ```


!!! note
    If several tasks have the same tag, only one of them is cancelled. If you
    want to cancel all the tasks with the same tag, you can use loop until
    `cancel_by_task` returns zero:
```
    while m.cancel_by_taskid("my-tag")>0:
        pass
```


### Blocking workers

You may find that certain hosts are not correctly configured to run your
tasks. The manager can be directed to ignore certain workers, as:

=== "Python"
    ```python
    t = m.wait(5)

    # if t fails given a worker misconfiguration:
    m.block_host(t.hostname)
    ```

=== "C"
    ```C
    struct vine_task *t = vine_wait(m, t);

    //if t fails given a worker misconfiguration:
    vine_block_host(m, vine_task_get_hostname(t));
    ```

### Performance Statistics

The manager tracks a fair number of statistics that count the number of tasks,
number of workers, number of failures, and so forth. This information is useful
to make a progress bar or other user-visible information:

=== "Python"
    ```python
    stats = m.stats
    print(stats.workers_busy)
    ```

=== "C"
    ```C
    struct vine_stats stats;
    vine_get_stats(m, &stats);
    printf("%d\n", stats->workers_connected);
    ```

In Python, you can also expose these statistics to a Prometheus server for
external monitoring. See [Prometheus Metrics](#prometheus-metrics).

