## Managing Resources

Unless otherwise specified, TaskVine assumes that a single task runs on a
single worker at a time, and a single worker occupies an entire machine.

However, if the resources at a machine are larger than what you know a task
requires, you most likely will want one worker to manage multiple tasks
running on that machine. For
example, if you have a 8-core machine, then you might want to run four 2-core
tasks on a single worker at once, being careful not to exceed the available
memory and disk.

### Task Resources

To run several tasks in a worker, every task must have a description of the
resources it uses, in terms of cores, memory, disk, and gpus. While time is
not exactly a type of resource, specifying the running time of tasks can
often be helpful to map tasks to workers. These resources can be specified
as in the following example:

=== "Python"
    ```python
    t.set_cores(1)           # task needs one core
    t.set_memory(1024)       # task needs 1024 MB of memory
    t.set_disk(4096)         # task needs 4096 MB of disk space
    t.set_gpus(0)            # task does not need a gpu
    t.set_time_max(100)      # task is allowed to run in 100 seconds
    t.set_time_min(10)       # task needs at least 10 seconds to run (see vine_worker --wall-time option above)
    t.add_feature("NVIDIA RTX A2000")  # task requires this specific GPU type

    # these can be set when the task is declared as well:
     t = vine.Task(
        command = "./gzip < my-file > my-file.gz",
        cores = 1,
        memory = 1024,
        disk = 4096,
        gpus = 0,
        time_max = 100,
        time_min = 10
    )
    ```

=== "C"
    ```C
    vine_task_set_cores(t, 1)             # task needs one core
    vine_task_set_memory(t, 1024)         # task needs 1024 MB of memory
    vine_task_set_disk(t, 4096)           # task needs 4096 MB of disk space
    vine_task_set_gpus(t, 0)              # task does not need a gpu
    vine_task_set_run_time_max(t, 100)    # task is allowed to run in 100 seconds
    vine_task_set_run_time_min(t, 10)     # task needs at least 10 seconds to run (see vine_worker --wall-time option above)
    vine_task_add_feature(t, "NVIDIA RTX A2000")  # task requires this specific GPU type
    ```

When the maximum running time is specified, TaskVine will kill any task that
exceeds its maximum running time. The minimum running time, if specified, helps
TaskVine decide which worker best fits which task.  Specifying tasks' running
time is especially helpful in clusters where workers may have a hard threshold
of their running time.

Resources are allocated according to the following rules:

1. If the task does not specify any resources, then it is allocated a whole worker.
2. The task will be allocated as least as much of the value of the resources
  specified. E.g., a task that specifies two cores will be allocated at
  least two cores.
3. If gpus remain unspecified, then the task is allocated zero gpus.
4. If a task specifies gpus, but does not specify cores, then the task is allocated zero cores.
5. In all other cases, cores, memory, and disk of the worker are divided
  evenly according to the maximum proportion of specified task
  requirements over worker resources. The proportions are rounded up so that
  only whole number of tasks could fit in the worker.

As an example, consider a task that only specifies 1 core, and does not specify
any other resource, and a worker with 4 cores, 12 GB of memory, and 36 GB of
disk. According to the rules above:

- Rule 1 does not apply, as at least one resource (cores) was specified.
- According to rule 2, the task will get at least one core.
- According to rule 3, the task will not be allocated any gpus.
- Rule 4 does not apply, as no gpus were specified, and cores were specified.
- For rule 5, the task requires 1 core, and the worker has 4 cores. This gives a proportion
  of 1/4=0.25. Thus, the task is assigned 25% of the memory and disk (3 GB and
  9 GB respectively).

As another example, now assume that the task specifies 1 cores and 6 GB of memory:

- Rules 1 to 4 are as the last example, only that now the task will get at
  least 6 GB of memory.
- From cores we get a proportion of 1/4=0.25, and from memory 6GB/12GB=0.5.
  The memory proportion dictates the allocation as it is the largest. This
  means that the task will get assigned 50% of the cores (2), memory
  (6 GB), and disk (18 GB).

Note that proportions are 'rounded up', as the following example shows.
Consider now that the task requires 1 cores, 6GB of memory, and 27 GB of disk:

- Rules 1 to 4 are as before, only that now the worker will get at
  least 30 GB of disk.
- The proportions are 1/4=0.25 for cores, 6GB/12GB=0.5 for memory, and
  27GB/36GB=0.75 for disk. This would assign 3 cores, 9 memory, and 27
  to the task. However, this would mean that no other task of this size would
  be able to run in the worker. Rather than assign 75% of the resources and
  risk an preventable failure because of resource exhaustion, the task is
  assigned 100% of the resources from the worker. More generally, allocations
  are rounded up so that only a whole number of tasks can be fit in the worker.

!!! note
    If you want TaskVine to exactly allocate the resources you have
    specified, use `m.disable_proportional_resources()` (see also `proportional-whole-tasks`
    [here](#tuning-specialized-execution-parameters).  In
    general, however, we have found that using proportions nicely adapts to the
    underlying available resources, and leads to very few resource exhaustion
    failures while still using worker resources efficiently.

The current TaskVine implementation only accepts whole integers for its
resources, which means that no worker can concurrently execute more tasks than
its number of cores. (This will likely change in the future.)

When you would like to run several tasks in a worker, but you are not sure
about the resources each task needs, TaskVine can automatically find values
of resources that maximize throughput, or minimize waste. This is discussed in
the section [below](#grouping-tasks-with-similar-resource-needs).

### Worker Resources

By default, a worker tries to use all the resources of the machine it is
running.  The resources detected are displayed when the worker starts up,
for example:

```
vine_worker: creating workspace /tmp/worker-102744-8066
vine_worker: using 16 cores, 15843 MB memory, 61291 MB disk, 0 gpus
```

You can manually adjust the resources managed by a worker like this:


```sh
$ vine_worker --cores 8  --memory 1000 --disk 8000 --gpus 1 ...other options...
```

Unlike other resources, the default value for gpus is 0. You can use the
command line option `--gpus` to declare how many gpus are available at a
worker.

When the lifetime of the worker is known, for example, the end of life of a
lease, this information can be communicated to the worker as follows. For
example, if the worker will be terminated in one hour:

```sh
$ vine_worker --wall-time 3600 ...other options...
```

In combination with the worker option `--wall-time`, tasks can request a
minimum time to execute with `set_time_min`, as explained (below)[#setting-task-resources].

You may also use the same `--cores`, `--memory`, `--disk`, and `--gpus` options when using
batch submission script `vine_submit_workers`, and the script will correctly ask the right
batch system for a node of the desired size.

The only caveat is when using `vine_submit_workers -T uge`, as there are many
differences across systems that the script cannot manage. For `
vine_submit_workers -T uge` you have to set **both** the resources used by the
worker (i.e., with `--cores`, etc.) and the appropriate computing node with the `
-p ` option.

For example, say that your local UGE installation requires you to set the
number of cores with the switch ` -pe smp ` , and you want workers with 4
cores:

```sh
$ vine_submit_workers -T uge --cores 4 -p "-pe smp 4" MACHINENAME 9123
```

If you find that there are options that are needed every time, you can compile
CCTools using the ` --uge-parameter `. For example, at Notre Dame we
automatically set the number of cores as follows:

```sh
$ ./configure  --uge-parameter '-pe smp $cores'
```


So that we can simply call:

```sh
$ vine_submit_workers -T uge --cores 4 MACHINENAME 9123
```

The variables `$cores `, `$memory `, and `$disk `, have the values of the
options passed to `--cores`, `--memory`, `--disk. `

### Factory Resources

The `vine_factory` accepts the arguments `--cores`, `--memory`,
`--disk`, and `--gpus` to set the size of the desired workers. Resources may also be
set in the configuration file as follows:

```json
{
    "manager-name": "myproject",
    "max-workers": 4,
    "min-workers": 1,
    "cores": 4,
    "memory": 4096,
    "disk": 4096,
    "gpus": 1
}
```

### GPU Types and Custom Features

It is sometimes necessary to match a task to a worker that has a specific capability.
Perhaps your pool of workers has two different kinds of GPUs.
The type of a GPU is automatically reported as a "feature" that tasks can select.

To describe a task that can only run on a specific GPU type, use `add_feature`:

=== "Python"
    ```python
    t.add_feature("NVIDIA RTX A2000") # task requires worker with this feature
    ```

=== "C"
    ```C
    vine_task_add_feature(t, "NVIDIA RTX A2000") # task requires worker with this feature
    ```

(Note that the GPU feature is automatically reported by the worker
when it starts up.)

```
vine_worker: using 4 cores, 15610 MB memory, 33859 MB disk, 1 gpus
vine_worker: gpu is called feature "NVIDIA RTX A2000"
```

You may also add additional custom features to a worker at startup time
using the `--feature` option:

```
vine_worker ... --feature alpha --feature beta ...
```

Or, use the factory to start a large number of workers with that feature:

```
vine_factory ... --feature alpha --feature beta ...
```

### Monitoring and Enforcement

So far we have used resources values simply as hints to TaskVine to schedule
concurrent tasks at workers. By default, TaskVine does not monitor or enforce
these limits. You can enable monitoring and enforcement as follows:

=== "Python"
    ```python
    # Measure the resources used by tasks, and terminate tasks that go above their
    # resources:
    m.enable_monitoring()

    # Measure the resources used by tasks, but do not terminate tasks that go above
    # declared resources:
    m.enable_monitoring(watchdog=False)

    # Measure the resources used by tasks, but do not terminate tasks that go
    # above declared resources, and generate a time series per task. These time
    # series are written to the logs directory `vine-logs/time-series`.
    # Use with caution, as time series for long running tasks may be in the
    # order of gigabytes.
    m.enable_monitoring(m, watchdog=False, time_series=True)
    ```

=== "C"
    ```C
    /* Measure the resources used by tasks, and terminate tasks that go above their
    resources: */
    vine_enable_monitoring(m, 1, 0)

    /* Measure the resources used by tasks, but do not terminate tasks that go above
    declared resources: */
    vine_enable_monitoring(m, 0, 0)

    /* Measure the resources used by tasks, but do not terminate tasks that go
    above # declared resources, and generate a time series per task. These time
    series are written to the logs directory `vine-logs/time-series`.
    Use with caution, as time series for long running tasks may be in the
    order of gigabytes. */
    vine_enable_monitoring(m, 0, 1)
    ```

When monitoring is enabled, you can explore the resources measured when a task
returns:

=== "Python"
    ```python
    t = m.wait(5)
    if t:
        print("Task used {} cores, {} MB memory, {} MB disk",
            t.resources_measured.cores,
            t.resources_measured.memory,
            t.resources_measured.disk)
        print("Task was allocated {} cores, {} MB memory, {} MB disk",
            t.resources_requested.cores,
            t.resources_requested.memory,
            t.resources_requested.disk)
        if t.limits_exceeded and t.limits_exceeded.cores > -1:
            print("Task exceeded its cores allocation.")
    ```

=== "C"
    ```C
    vine_task *t = vine_wait(m, 5);
    if(t) {
        const struct rmsummary *measured  = vine_task_get_resources(t, "measured");
        const struct rmsummary *requested = vine_task_get_resources(t, "requested");
        const struct rmsummary *allocated = vine_task_get_resources(t, "allocated");

        printf("Task used %f cores, %f MB memory, %f MB disk",
            measured->cores,
            measured->memory,
            measured->disk);
        printf("Task was allocated %f cores, %f MB memory, %f MB disk",
            requested->cores,
            requested->memory,
            requested->disk});
        if(measured->limits_exceeded && measured->limits_exceeded->cores > -1) {
            printf("Task exceeded its cores allocation.")
        }
    }
    ```

Alternatively, when you declare a task (i.e., before submitting it), you can
declare a directory to which a report of the resources will be written. The
report format is JSON, as its filename has the form
`vine-PID_OF_MANAGER-task-TASK_ID.summary`.

=== "Python"
    ```python
    t = vine.Task(...)
    t.set_monitor_output("my-resources-output")
    ...
    taskid = m.submit(t)

    # this can be set at declaration as well:
     t = vine.Task(
        command = ...,
        monitor_output = "my-resources-output"
     )
    ```

=== "C"
    ```C
    struct vine_task *t = vine_task_create(...);
    vine_task_set_monitor_output(t, "my-resources-output");
    ...
    int taskid = vine_submit(m, t);
    ```

TaskVine also measures other resources, such as peak `bandwidth`,
`bytes_read`, `bytes_written`, `bytes_sent`, `bytes_received`,
`total_files`, `cpu_time`, and `wall_time`.


### Grouping Tasks with Similar Resource Needs

Several tasks usually share the same resource description, and to this end,
TaskVine allows you to tasks into groups called **categories**. You can
attach resource descriptions to each category, and then label a task to set it
as part of a category.

We can create some categories with their resource description as follows:

=== "Python"
    ```python
    # memory and disk values in MB.
    m.set_category_resources_max('my-category-a', {'cores': 2, 'memory': 1024, 'disk': 2048, 'gpus': 0})
    m.set_category_resources_max('my-category-b', {'cores': 1})
    m.set_category_resources_max('my-category-c', {})
    ```

=== "C"
    ```C
    # memory and disk values in MB.
    struct rmsummary *ra = rmsummary_create(-1);
    ra->cores = 2;
    ra->memory = 1024;
    ra->disk = 2048;
    vine_set_resources_max("my-category-a", ra);
    rmsummary_delete(ra);

    struct rmsummary *rb = rmsummary_create(-1);
    rb->cores = 1;
    vine_set_resources_max("my-category-b", rb);
    rmsummary_delete(rb);

    vine_set_resources_max("my-category-c", NULL);
    ```

In the previous examples, we created three categories. Note that it is not
necessary to set all the resources, as TaskVine can be directed to
compute some efficient defaults. To assign a task to a category:

=== "Python"
    ```python
    t.set_category('my-category-a')

    # alternatively:
     t = vine.Task(
        command = ...,
        category = 'my-category-a'
     )
    ```

=== "C"
    ```C
    vine_task_set_category(t, "my-category-a")
    ```
When a category leaves some resource unspecified, then TaskVine tries to find
some reasonable defaults in the same way described before in the section
(Specifying Task Resources)[#setting-task-resources].

!!! warning
    When a task is declared as part of a category, and also has resources
    set directly with calls such as `t.set_cores`, the resources
    directly set take precedence over the category declaration for that
    task

When the resources used by a task are unknown, TaskVine can measure and
compute efficient resource values to maximize throughput or minimize waste, as
we explain in the following sections.

### Automatic Resource Management

If the resources a category uses are unknown, then TaskVine can be directed
to find efficient resource values to maximize throughput or minimize resources
wasted. In these modes, if a value for a resource is set with
`set_resources_max`, then it is used as a theoretical maximum.

When automatically computing resources, if any of cores, memory or disk are
left unspecified in `set_resources_max`, then TaskVine will run some
tasks using whole workers to collect some resource usage statistics. If all
cores, memory, and disk are set, then TaskVine uses these maximum
values instead of using whole workers. As before, unspecified gpus default to 0.

Once some statistics are available, further tasks may run with smaller
allocations if such a change would increase throughput. Should a task exhaust
its resources, it will be retried using the values of `set_resources_max`,
or a whole worker, as explained before.

Automatic resource management is enabled per category as follows:

=== "Python"
    ```python
    m.enable_monitoring()
    m.set_category_resources_max('my-category-a', {})
    m.set_category_mode('my-category-a', "max throughput")

    m.set_category_resources_max('my-category-b', {'cores': 2})
    m.set_category_mode('my-category-b', "max throughput")
    ```

=== "C"
    ```C
    vine_enable_monitoring(m, 0, 0);
    vine_set_category_resources_max(m, "my-category-a", NULL);
    vine_set_category_mode(m, "my-category-a", VINE_ALLOCATION_MODE_MAX_THROUGHPUT);

    struct rmsummary *r = rmsummary_create(-1);
    r->cores = 2;
    vine_set_category_resources_max(m, "my-category-b", r);
    vine_set_category_mode(m, "my-category-b", VINE_ALLOCATION_MODE_MAX_THROUGHPUT);
    rmsummary_delete(r);
    ```

In the previous examples, tasks in 'my-category-b' will never use more than two
cores, while tasks in 'my-category-a' are free to use as many cores as the
largest worker available if needed.

You can set a limit on the minimum resource value a category can use. The
automatic resource computation will never go below the values set:

=== "Python"
    ```python
    m.set_category_resources_min('my-category-a', {'memory': 512})
    ```

=== "C"
    ```C
    struct rmsummary *r = rmsummary_create(-1);
    r->memory = 512;
    vine_set_category_resources_min(m, "my-category-a", r);
    rmsummary_delete(r);
    ```

You can enquire about the resources computed per category with
`vine_status`:


```
$ vine_status -A  IP-OF-MACHINE-HOSTING-WQ PORT-OF-WQ
CATEGORY        RUNNING    WAITING  FIT-WORKERS  MAX-CORES MAX-MEMORY   MAX-DISK
analysis            216        784           54          4      ~1011      ~3502
merge                20         92           30         ~1      ~4021      21318
default               1         25           54         >1       ~503       >243
```

In the above, we have three categories, with RUNNING and WAITING tasks. The
column FIT-WORKERS shows the count of workers that can fit at least one task in
that category using the maximum resources either set or found. Values for max
cores, memory and disk have modifiers `~` and `>` as follows:

- No modifier: The maximum resource usage set with `set_category_resources_max`, or set for any task in the category via calls such as `set_cores`.
- ~: The maximum resource usage so far seen when resource is left unspecified in `set_category_resources_max`. All tasks so far have run with no more than this resource value allocated.
- >: The maximum resource usage that has caused a resource exhaustion. If this value is larger than then one set with `set_category_resources_max`, then tasks that exhaust resources are not retried. Otherwise, if a maximum was not set, the tasks will be retried in larger workers as workers become available.


!!! warning
    When resources are set directly to the task with calls such as
    `t.set_cores`, such resources are fixed for the task and are not
    modified when more efficient values are found.


