# Work Queue User's Manual

## Overview

Work Queue is a framework for building large scale master-worker applications.
Using the Work Queue library, you create a custom master program that defines
and submits a large number of small tasks. Each task is distributed to a
remote worker process which executes it and returns the results. As results
are created, the master may generate more tasks to be executed. It is not
unusual to write programs that distribute millions of tasks to thousands of
remote workers.

Each worker process is a common executable that can be deployed within
existing cluster and cloud systems, so it's easy to deploy a Work Queue
application to run on machines that you already have access to. Whether you
use a university batch system or a commercial cloud provider, your Work Queue
application will be able to run there.

Work Queue is a production framework that has been used to create highly
scalable scientific applications in high energy physics, bioinformatics, data
mining, and other fields. It can also be used as an execution system for the
[Makeflow](http://ccl.cse.nd.edu/software/makeflow) workflow engine. To see
some of the Work Queue applications running right now, view the [real time
status page](http://ccl.cse.nd.edu/software/workqueue/status).

## Getting Started

## Installing Work Queue

Work Queue is part of the [Cooperating Computing
Tools](http://ccl.cse.nd.edu/software). The CCTools package can be downloaded
from [this web page](http://ccl.cse.nd.edu/software/download). Follow the
[installation instructions](../install) to setup CCTools required for running
Work Queue. The documentation for the full set of features of the Work Queue
API can be viewed from either within the CCTools package or
[here](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html) and
[here](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceWorkQueuePython.html).

## Building a Work Queue Application

We begin by running a simple but complete example of a Work Queue application.
After trying it out, we will then show how to write a Work Queue application
from scratch.

We assume that you have downloaded and installed the cctools package in your
home directory under `$HOME/cctools`. Next, download the example file for the
language of your choice:

  * Python: [work_queue_example.py](examples/work_queue_example.py)
  * Perl: [work_queue_example.pl](examples/work_queue_example.pl)
  * C: [work_queue_example.c](examples/work_queue_example.c)

If you are using the Python example, set `PYTHONPATH` to include the Python
modules in cctools: (You may need to adjust the version number to match your
Python.)
    
```sh
$ PYVER=$(python -c 'import sys; print("%s.%s" % sys.version_info[:2])')
$ export PYTHONPATH=${PYTHONPATH}:${HOME}/cctools/lib/python${PYVER}/site-packages
```

If you are using the Perl example, set `PERL5LIB` to include the Perl modules in
cctools:

```sh
$ export PERL5LIB=${PERL5LIB}:${HOME}/cctools/lib/perl5/site_perl
```


If you are using the C example, compile it like this:

```sh
$ gcc work_queue_example.c -o work_queue_example -I${HOME}/cctools/include/cctools -L${HOME}/cctools/lib -lwork_queue -ldttools -lm -lz
```
    
    

## Running a Work Queue Application

The example application simply compresses a bunch of files in parallel. The
files to be compressed must be listed on the command line. Each will be
transmitted to a remote worker, compressed, and then sent back to the Work
Queue master. To compress files `a`, `b`, and `c` with this example
application, run it as:

```sh
# Python:
$ ./work_queue_example.py a b c

# Perl:
$ ./work_queue_example.pl a b c

# C
$ ./work_queue_example a b c
```
    

You will see this right away:


```sh
listening on port 9123...
submitted task: /usr/bin/gzip < a > a.gz
submitted task: /usr/bin/gzip < b > b.gz
submitted task: /usr/bin/gzip < c > c.gz
waiting for tasks to complete...
```
    

The Work Queue master is now waiting for workers to connect and begin
requesting work. (Without any workers, it will wait forever.) You can start
one worker on the same machine by opening a new shell and running:

    
```sh
# Substitute the name of your machine for MACHINENAME.
$ work_queue_worker MACHINENAME 9123
```

If you have access to other machines, you can `ssh` there and run workers as
well. In general, the more you start, the faster the work gets done. If a
worker should fail, the work queue infrastructure will retry the work
elsewhere, so it is safe to submit many workers to an unreliable system.

If you have access to a Condor pool, you can use this shortcut to submit ten
workers at once via Condor:

```sh
$ condor_submit_workers MACHINENAME 9123 10

Submitting job(s)..........
Logging submit event(s)..........
10 job(s) submitted to cluster 298.
```
    

Or, if you have access to an SGE cluster, do this:

```sh
$ sge_submit_workers MACHINENAME 9123 10

Your job 153083 ("worker.sh") has been submitted
Your job 153084 ("worker.sh") has been submitted
Your job 153085 ("worker.sh") has been submitted
...
```
    

Similar scripts are available for other common batch systems:

```sh
$ pbs_submit_workers MACHINENAME 9123 10

$ torque_submit_workers MACHINENAME 9123 10

$ slurm_submit_workers MACHINENAME 9123 10

$ ec2_submit_workers MACHINENAME 9123 10
```

When the master completes, if the workers were not shut down in the master,
your workers will still be available, so you can either run another master
with the same workers, or you can remove the workers with `kill`, `condor_rm`,
or `qdel` as appropriate. If you forget to remove them, they will exit
automatically after fifteen minutes. (This can be adjusted with the `-t`
option to `worker`.)

## Writing a Work Queue Master Program

The easiest way to start writing your own program using WorkQueue is to modify
with one of the examples in [Python](examples/work_queue_example.py),
[Perl](examples/work_queue_example.pl), or [C](examples/work_queue_example.c).
The basic outline of a WorkQueue master is:

1. Create and configure the tasks' queue.
2. Create tasks and add them to the queue.
3. Wait for tasks to complete.

To create a new queue listening on port 9123:

#### Python

```python
import work_queue as wq

# create a new queue listening on port 9123
q = wq.WorkQueue(9123)
```

#### Perl

```perl
use Work_Queue;

# create a new queue listening on port 9123
my $q = Work_Queue->new(9123);
```

#### C

```C
#include "work_queue.h"
    
# create a new queue listening on port 9123
struct work_queue *q = work_queue_create(9123);
```
    
The master then creates tasks to submit to the queue. Each task consists of a
command line to run and a statement of what data is needed, and what data will
be produced by the command. Input data can be provided in the form of a file
or a local memory buffer. Output data can be provided in the form of a file or
the standard output of the program. It is also required to specify whether the
data, input or output, need to be cached at the worker site for later use.

In the example, we specify a command `./gzip` that takes a single input file
`my-file` and produces a single output file `my-file.gz`. We then create a task
by providing the specified command as an argument:

#### Python

```python
t = wq.Task("./gzip < my-file > my-file.gz")
```

#### Perl

```perl
my $t = Work_Queue::Task->new("./gzip < my-file > my-file.gz");
```

#### C

```C
struct work_queue_task *t = work_queue_task_create("./gzip < my-file > my-file.gz");
```
    
The input and output files associated with the execution of the task must be
explicitly specified. In the example, we also specify the executable in the
command invocation as an input file so that it is transferred and installed in
the working directory of the worker. We require this executable to be cached
so that it can be used by subsequent tasks that need it in their execution. On
the other hand, the input and output of the task are not required to be cached
since they are not used by subsequent tasks in this example.


#### Python
    
```python
# t.specify_input_file("name at master", "name when copied at execution site", ...)

t.specify_input_file("/usr/bin/gzip", "gzip",       cache = True)
t.specify_input_file("my-file",       "my-file",    cache = False)
t.specify_output_file("my-file.gz",   "my-file.gz", cache = False)

# when the name at master is the same as the exection site, we can write instead:
t.specify_input_file("my-file",     cache = False)
t.specify_output_file("my-file.gz", cache = False)
```

#### Perl
    
```perl
# $t->specify_input_file(local_name => "name at master", remote_name => "name when copied at execution site", ...);

$t->specify_input_file(local_name => "/usr/bin/gzip", remote_name => "gzip",       cache = True);
$t->specify_input_file(local_name => "my-file",       remote_name => "my-file",    cache = False);
$t->specify_output_file(local_name => "my-file.gz",   remote_name => "my-file.gz", cache = False);

# when the name at master is the same as the exection site, we can write instead:
$t->specify_input_file(local_name => "my-file",     cache = False);
$t->specify_output_file(local_name => "my-file.gz", cache = False);
```


#### C

```C
# work_queue_task_specify_file(t, "name at master", "name when copied at execution site", ...)

work_queue_task_specify_file(t, "/usr/bin/gzip", "gzip",       WORK_QUEUE_INPUT,  WORK_QUEUE_CACHE);
work_queue_task_specify_file(t, "my-file",       "my-file",    WORK_QUEUE_INPUT,  WORK_QUEUE_NOCACHE);
work_queue_task_specify_file(t, "my-file.gz",    "my-file.gz", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE);
```

Note that the specified input directories and files for each task are
transferred and setup in the sandbox directory of the worker (unless an
absolute path is specified for their location). This sandbox serves as the
initial working directory of each task executed by the worker. The task
outputs are also stored in the sandbox directory (unless an absolute path is
specified for their storage). The path of the sandbox directory is exported to
the execution environment of each worker through the `WORK_QUEUE_SANDBOX` shell
environment variable. This shell variable can be used in the execution
environment of the worker to describe and access the locations of files in the
sandbox directory.

We can also run a program that is already installed at the remote site, where
the worker runs, by specifying its installed location in the command line of
the task (and removing the specification of the executable as an input file).
For example:

#### Python

```python
t = Task("/usr/bin/gzip < my-file > my-file.gz")
```

#### Perl

```perl
my $t = Work_Queue::Task->new("/usr/bin/gzip < my-file > my-file.gz")
```

#### C

```C
struct work_queue_task *t = work_queue_task_create("/usr/bin/gzip < my-file > my-file.gz");
```

Once a task has been fully specified, it can be submitted to the queue where it
gets assigned a unique taskid:

### Python

```python
taskid = q.submit(t)
```

### Perl

```perl
my $taskid = $q->submit($t)
```

#### C

```C
int taskid = work_queue_submit(q,t);
```

Next, wait for a task to complete, stating how long you are willing to wait for
a result, in seconds:

#### Python
    
```python
while not q.empty():
    t = q.wait(5)
    if t:
        print("Task {} has returned!".format(t.id))

        if t.return_status == 0:
            print("command exit code:\n{}".format(t.exit_code))
            print("stdout:\n{}".format(t.output))
        else:
            print("There was a problem executing the task.")
```

#### Perl
    
```perl
while(not $q->empty()) {
    my $t = $q->wait(5)
    if($t) {
        print("Task @{[$t->id]} has returned!\n");

        if($t->{return_status} == 0) {
            print("command exit code:\n@{[$t->{exit_code}]}\n");
            print("stdout:\n@{[$t->{output}]}\n");
        } else {
            print("There was a problem executing the task.\n");
        }
    }
}
```

#### C

```C
while(!work_queue_empty(q)) {
    struct work_queue_task *t = work_queue_wait(q,5);
    if(t) {
        printf("Task %d has returned!\n", t->taskid);
        if(t->return_status == 0) {
            printf("command exit code: %d\n", t->exit_code);
            printf("stdout: %s\n", t->output);
        } else {
            printf("There was a problem executing the task.\n");
        }

    }
}
```

A completed task will have its output files written to disk. You may examine
the standard output of the task in `output` and the exit code in
`exit_status`.

!!! note
    The size of `output` is limited to 1 GB. Any output beyond 1 GB will be
    truncated. So, please redirect the stdout `./my-command > my-stdout` of the
    task to a file and specify the file as an output file of the task as
    described above.

When you are done with the task, delete it (only needed for C):

#### C
```C
work_queue_task_delete(t);
```

Continue submitting and waiting for tasks until all work is complete. You may
check to make sure that the queue is empty with `work_queue_empty`. When all
is done, delete the queue (only needed for C):

#### C

```C
work_queue_delete(q);
```
    

Full details of all of the Work Queue functions can be found in the [Work Queue API](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html).


## Project Names and the Catalog Server

Keeping track of the master's hostname and port can get cumbersome, especially
if there are multiple masters. To help with difficulty, we provide the project
name feature to identify a Work Queue master with a more recognizable project
name. Work Queue workers can then be started for their masters by providing
the project names.

The project name feature uses the **catalog server** to maintain and track the
project names of masters and their respective locations. It works as follows:
the master advertises its project name along with its hostname and port to the
catalog server. Work Queue workers that are provided with the master's project
name query the catalog server to find the hostname and port of the master with
the given project name. So, to utilize this feature, the master must be
specified to run in the `WORK_QUEUE_MASTER_MODE_CATALOG`. See [Catalog
Servers](../catalog) for details on specifying catalog servers.

For example, to have a Work Queue master advertise its project name as
`myproject`, add the following code snippet after creating the queue:

#### Python
```python
q = wq.WorkQueue(name = "myproject")
```

#### Perl
```perl
my $q = Work_Queue->new(name => "myproject");
```

#### C

```C
work_queue_specify_name(q, "myproject");
```
    

    
To start a worker for this master, specify the project name (`myproject`) to
connect in the `-M` option:

```sh
$ work_queue_worker -M myproject
```
    

You can start ten workers for this master on Condor using
`condor_submit_workers` by providing the same option arguments.:
    
```sh
$ condor_submit_workers -M myproject 10
Submitting job(s)..........
Logging submit event(s)..........
10 job(s) submitted to cluster 298.
```

Or similarly on SGE using `sge_submit_workers` as:

```sh
$ sge_submit_workers -M myproject 10
Your job 153097 ("worker.sh") has been submitted
Your job 153098 ("worker.sh") has been submitted
Your job 153099 ("worker.sh") has been submitted
...
```
    

## Running Multiple Tasks per Worker

Unless otherwise specified, Work Queue assumes that a single task runs on a
single worker at a time, and a single worker occupies an entire machine.

However, if you have large multi-core machines and multi-threaded tasks, you
will want one worker to manage multiple tasks running on a machine. For
example, if you have a 8-core machine, then you might want to run four 2-core
tasks on a single worker at once, being careful not to exceed the available
memory and disk.

By default a worker tries to use all the resources of the machine it is
running.  You can specify the exact number of cores, memory, and disk to use
like this:
    
```sh
$ work_queue_worker --cores 8  --memory 1000 --disk 8000 -M myproject
```
    
To run several task in a worker, every task must have a description of the
resources it uses, in terms of cores, memory, and disk:


#### Python

```python
t.specify_cores(2)    #needs 2 cores
t.specify_memory(100) #needs 100 MB memory
t.specify_disk(1000)  #needs 1 GB disk
```

#### Perl

```perl
$t->specify_cores(2);    #needs 2 cores
$t->specify_memory(100); #needs 100 MB memory
$t->specify_disk(1000);  #needs 1 GB disk
```

#### C

```C
work_queue_task_specify_cores(t, 2);    //needs 2 cores
work_queue_task_specify_memory(t, 100); //needs 100 MB memory
work_queue_task_specify_disk(t, 1000);  //needs 1 GB disk
```

Note that if no requirements are specified, a task consumes an entire worker.
**All resource requirements must be specified in order to run multiple tasks on
a single worker.** For example, if you annotate a task as using 1 core, but
don't specify its memory or disk requirments, Work Queue will only schedule one
task to a two-core worker. However, if you annotate the core, memory, and disc
requirements for a task, Work Queue can schedule two such tasks to a two- task
worker, assuming it has the available memory and disk requirements for each
individual task.

You may also use the `--cores`, `--memory`, and `--disk` options when using
batch submission scripts such as `condor_submit_workers` or
`slurm_submit_workers`, and the script will correctly ask the batch system for
an appropiate node.

The only caveat is when using `sge_submit_workers`, as there are many
differences across systems that the script cannot manage. For `
sge_submit_workers ` you have to specify **both** the resources used by the
worker (i.e., with `--cores`, etc.) and the appropiate computing node with the `
-p ` option.

For example, say that your local SGE installation requires you to specify the
number of cores with the switch ` -pe smp ` , and you want workers with 4
cores:

```sh
$ sge_submit_workers --cores 4 -p "-pe smp 4" MACHINENAME 9123
```

If you find that there are options that are needed everytime, you can compile
CCTools using the ` --sge-parameter `. For example, at Notre Dame we
automatically set the number of cores as follows:

```sh
$ ./configure  --sge-parameter '-pe smp $cores'
```
    

So that we can simply call:

```sh
$ sge_submit_workers --cores 4 MACHINENAME 9123
```
    
The variables `$cores `, `$memory `, and `$disk `, have the values of the
options passed to `--cores`, `--memory`, `--disk. `


## Recommended Practices

### Security

By default, Work Queue does **not** perform any authentication, so any workers
will be able to connect to your master, and vice versa. This may be fine for a
short running anonymous application, but is not safe for a long running
application with a public name.

We recommend that you enable a password for your applications. Create a file
(e.g. ` mypwfile`) that contains any password (or other long phrase) that you
like (e.g. `This is my password`). The password will be particular to your
application and should not match any other passwords that you own. Note that
the contents of the file are taken verbatim as the password; this means that
any new line character at the end of the phrase will be considered as part of
the password.

Then, modify your master program to use the password:

#### Python

```python
q.specify_password_file("mypwfile")
```

#### Perl

```perl
$q->specify_password_file("mypwfile");
```

#### C

```C
work_queue_specify_password_file(q,"mypwfile");
```
    

And give the `--password` option to give the same password file to your
workers:
    
```sh
$ work_queue_worker --password mypwfile -M myproject
```

With this option enabled, both the master and the workers will verify that the
other has the matching password before proceeding. The password is not sent in
the clear, but is securely verified through a SHA1-based challenge-response
protocol.

### Debugging

Work Queue can be set up to print debug messages at the master and worker to
help troubleshoot failures, bugs, and errors. To activate debug output:


#### Python

```python
q = wq.WorkQueue(debug_log = "my.debug.log")
```

#### Perl

```perl
my $q = Work_Queue->new(debug_log => "my.debug.log");
```

#### C

```C
#include "debug.h"

cctools_debug_flags_set("all");
cctools_debug_config_file("my.debug.log");
```
    
The `all` flag causes debug messages from every subsystem called by Work Queue
to be printed. More information about the debug flags are
[here](http://ccl.cse.nd.edu/software/manuals/api/html/debug_8h.html).


To enable debugging at the worker, set the `-d` option:

    
```sh
$ work_queue_worker -d all -o worker.debug -M myproject
```


### Logging and Plotting

You can specify a log file to obtain a time series of work queue statistics as
follows:

#### Python

```python
q = wq.WorkQueue(stats_log = "my.statslog")
```

#### Perl

```perl
my $q = Work_Queue->new(stats_log => "my.stats.log");
```

#### C

```C
work_queue_specify_log(q, "my.stats.log");
```

The script `work_queue_graph_log` is a wrapper for `gnuplot`, and with it you
can plot some of the statistics, such as total time spent transfering tasks,
number of tasks running, and workers connected:

```sh
$ work_queue_graph_log -o myplots my.stats.log
$ ls *.png
$ ... my.stats.log.tasks.png my.stats.log.tasks-log.png my.stats.log.time.png my.stats.log.time-log.png ...
```
    

We find it very helpful to plot these statistics when diagnosing a problem with
work queue applications.


## Advanced Topics

A variety of advanced features are available for programs with unusual needs
or very large scales. Each feature is described briefly here, and more details
may be found in the [Work Queue
API](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html).

### Pipelined Submission.

If you have a **very** large number of tasks to run, it may not be possible to
submit all of the tasks, and then wait for all of them. Instead, submit a
small number of tasks, then alternate waiting and submiting to keep a constant
number in the queue. The `hungry` will tell you if more submission are
warranted:

#### Python

```python
if q.hungry():
    # submit more tasks...
```

#### Perl

```perl
if($q->hungry()) {
    # submit more tasks...
}
```

#### C

```C
if(q->hungry()) {
    // submit more tasks...
}
```


### Watching Output Files

If you would like to see the output of a task as it is produced, add
`WORK_QUEUE_WATCH` to the flags argument of `specify_file`. This will
cause the worker to periodically send output appended to that file back to the
master. This is useful for a program that produces a log or progress bar as
part of its output.

#### Python
```python
t.specify_output_file("my-file", flags = wq.WORK_QUEUE_WATCH)
```

#### Perl
```perl
$t->specify_output_file(local_name => "my-file", flags = wq.WORK_QUEUE_WATCH)
```

#### C
```C
work_queue_task_specify_file(t, "my-file", "my-file", WORK_QUEUE_OUTPUT, WORK_QUEUE_NOCACHE | WORK_QUEUE_WATCH);
```

### Fast Abort

A large computation can often be slowed down by stragglers. If you have a
large number of small tasks that take a short amount of time, then Fast Abort
can help. The Fast Abort feature keeps statistics on tasks execution times and
proactively aborts tasks that are statistical outliers:

#### Python
```python
# Kill workers that are executing tasks twice as slow as compared to the
# average.
q.activate_fast_abort(2)
```

#### Perl
```perl
# Kill workers that are executing tasks twice as slow as compared to the
# average.
$q->activate_fast_abort(2);
```

#### C
```C
// Kill workers that are executing tasks twice as slow as compared to the
// average.
work_queue_activate_fast_abort(q, 2);
```

### String Interpolation

If you have workers distributed across multiple operating systems (such as
Linux, Cygwin, Solaris) and/or architectures (such as i686, x86_64) and have
files specific to each of these systems, this feature will help. The strings
$OS and $ARCH are available for use in the specification of input file names.
Work Queue will automatically resolve these strings to the operating system
and architecture of each connected worker and transfer the input file
corresponding to the resolved file name. For example:

#### Python
```C
t.specify_input_file("my-executable.$OS.$ARCH", "my-exe")
```

#### Perl
```perl
$t->specify_output_file(local_name => "my-executable.$OS.$ARCH", remote_name => "my-exe");
```

#### C

```C
work_queue_task_specify_file(t,"my-executable.$OS.$ARCH","./my-exe",WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
```

This will transfer `my-executable.Linux.x86_64` to workers running on a Linux
system with an x86_64 architecture and `a.Cygwin.i686` to workers on Cygwin
with an i686 architecture.

Note this feature is specifically designed for specifying and distingushing
input file names for different platforms and architectures. Also, this is
different from the $WORK_QUEUE_SANDBOX shell environment variable that exports
the location of the working directory of the worker to its execution
environment.


### Task Cancellations

This feature is useful in workflows where there are redundant tasks or tasks
that become obsolete as other tasks finish. Tasks that have been submitted can
be cancelled and immediately retrieved without waiting for Work Queue to
return them in `work_queue_wait`. The tasks to cancel can be identified by
either their `taskid` or `tag`. For example:

```python
# create task as usual and tag it with an arbitrary string.
t = wq.Task(...)
t.specify_tag("my-tag")

taskid = q.submit(t)

# cancel task by id. Return the canceled task.
t = q.cancel_by_taskid(taskid)

# or cancel task by tag. Return the canceled task.
t = q.cancel_by_tasktag("my-tag")
```


```perl
# create task as usual and tag it with an arbitrary string.
my $t = Work_Queue::Task->new(...)
my $t->specify_tag("my-tag")

my taskid = $q->submit($t);

# cancel task by id. Return the canceled task.
$t = $q->cancel_by_taskid(taskid);

# or cancel task by tag. Return the canceled task.
$t = $q->cancel_by_tasktag("my-tag");
```

```C
// create task as usual and tag it with an arbitrary string.
struct work_queue_task *t = work_queue_task_create("...");
work_queue_specify_task(t, "my-tag");

int taskid = work_queue_submit(q, t);

// cancel task by id. Return the canceled task.
t = work_queue_cancel_by_taskid(q, taskid);

# or cancel task by tag. Return the canceled task.
t = work_queue_cancel_by_tasktag(q, "my-tag");
```


!!! note
    If several tasks have the same tag, only one of them is cancelled. If you
    want to cancel all the tasks with the same tag, you can use loop until
    `cancel_by_task` does not return a task, as in:
```
    while q->cancel_by_taskid("my-tag"):
        pass
```


### Worker Blacklist

You may find that certain hosts are not correctly configured to run your
tasks. The master can be directed to ignore certain workers with the blacklist
feature. For example:

#### Python
```python
t = q.wait(5)

# if t fails given a worker misconfiguration:
q.blacklist(t.hostname)
```

#### Perl
```perl
my $t = $q->wait(5);

# if $t fails given a worker misconfiguration:
$q->blacklist($t->hostname);
```

#### C
```C
struct work_queue_task *t = work_queue_wait(q, t);

//if t fails given a worker misconfiguration:
work_queue_blacklist_add(q, t->{hostname});
```

### Performance Statistics

The queue tracks a fair number of statistics that count the number of tasks,
number of workers, number of failures, and so forth. This information is useful
to make a progress bar or other user-visible information:

#### Python
```python
stats = q.stats
print(stats.workers_busy)
```

#### Perl
```perl
my $stats = $q->stats;
print($stats->{task_running});
```

#### C
```C
struct work_queue_stats stats;
work_queue_get_stats(q, &stats);
printf("%d\n", stats->workers_connected);
```

The statistics available are:

| Field | Description
|-------|------------
|-      | **Stats for the current state of workers**
| workers_connected;	   | Number of workers currently connected to the master.
| workers_init;          | Number of workers connected, but that have not send their available resources report yet
| workers_idle;          | Number of workers that are not running a task.
| workers_busy;          | Number of workers that are running at least one task.
| workers_able;          | Number of workers on which the largest task can run.
| 
|-      | **Cumulative stats for workers**
| workers_joined;        | Total number of worker connections that were established to the master.
| workers_removed;       | Total number of worker connections that were released by the master, idled-out, fast-aborted, or lost.
| workers_released;      | Total number of worker connections that were asked by the master to disconnect.
| workers_idled_out;     | Total number of worker that disconnected for being idle.
| workers_fast_aborted;  | Total number of worker connections terminated for being too slow.
| workers_blacklisted ;  | Total number of workers blacklisted by the master. (Includes fast-aborted.)
| workers_lost;          | Total number of worker connections that were unexpectedly lost. (does not include idled-out or fast-aborted)
| 
|-      | **Stats for the current state of tasks**
| tasks_waiting;         | Number of tasks waiting to be dispatched.
| tasks_on_workers;      | Number of tasks currently dispatched to some worker.
| tasks_running;         | Number of tasks currently executing at some worker.
| tasks_with_results;    | Number of tasks with retrieved results and waiting to be returned to user.
| 
|-      | **Cumulative stats for tasks**
| tasks_submitted;            | Total number of tasks submitted to the queue.
| tasks_dispatched;           | Total number of tasks dispatch to workers.
| tasks_done;                 | Total number of tasks completed and returned to user. (includes tasks_failed)
| tasks_failed;               | Total number of tasks completed and returned to user with result other than WQ_RESULT_SUCCESS.
| tasks_cancelled;            | Total number of tasks cancelled.
| tasks_exhausted_attempts;   | Total number of task executions that failed given resource exhaustion.
| 
| - | **Master time statistics (in microseconds)**
| time_when_started;  | Absolute time at which the master started.
| time_send;          | Total time spent in sending tasks to workers (tasks descriptions, and input files.).
| time_receive;       | Total time spent in receiving results from workers (output files.).
| time_send_good;     | Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS.
| time_receive_good;  | Total time spent in sending data to workers for tasks with result WQ_RESULT_SUCCESS.
| time_status_msgs;   | Total time spent sending and receiving status messages to and from workers, including workers' standard output, new workers connections, resources updates, etc.
| time_internal;      | Total time the queue spents in internal processing.
| time_polling;       | Total time blocking waiting for worker communications (i.e., master idle waiting for a worker message).
| time_application;   | Total time spent outside work_queue_wait.
| 
| - | **Wrokers time statistics (in microseconds)**
| time_workers_execute;             | Total time workers spent executing done tasks.
| time_workers_execute_good;        | Total time workers spent executing done tasks with result WQ_RESULT_SUCCESS.
| time_workers_execute_exhaustion;  | Total time workers spent executing tasks that exhausted resources.
| 
| - | **Transfer statistics**
| bytes_sent;      | Total number of file bytes (not including protocol control msg bytes) sent out to the workers by the master.
| bytes_received;  | Total number of file bytes (not including protocol control msg bytes) received from the workers by the master.
|  bandwidth;       | Average network bandwidth in MB/S observed by the master when transferring to workers.
| 
| - | **Resources statistics**
| capacity_tasks;      | The estimated number of tasks that this master can effectively support.
| capacity_cores;      | The estimated number of workers' cores that this master can effectively support.
| capacity_memory;     | The estimated number of workers' MB of RAM that this master can effectively support.
| capacity_disk;       | The estimated number of workers' MB of disk that this master can effectively support.
| capacity_instantaneous;       | The estimated number of tasks that this master can support considering only the most recently completed task.
| capacity_weighted;   | The estimated number of tasks that this master can support placing greater weight on the most recently completed task.
| 
| total_cores;       | Total number of cores aggregated across the connected workers.
| total_memory;      | Total memory in MB aggregated across the connected workers.
| total_disk;	       | Total disk space in MB aggregated across the connected workers.
| 
| committed_cores;   | Committed number of cores aggregated across the connected workers.
| committed_memory;  | Committed memory in MB aggregated across the connected workers.
| committed_disk;	   | Committed disk space in MB aggregated across the connected workers.
| 
| max_cores;         | The highest number of cores observed among the connected workers.
| max_memory;        | The largest memory size in MB observed among the connected workers.
| max_disk;          | The largest disk space in MB observed among the connected workers.
| 
| min_cores;         | The lowest number of cores observed among the connected workers.
| min_memory;        | The smallest memory size in MB observed among the connected workers.
| min_disk;          | The smallest disk space in MB observed among the connected workers.
| 
| master_load;       | In the range of [0,1]. If close to 1, then the master is at full load and spends most of its time sending and receiving taks, and thus cannot accept connections from new workers. If close to 0, the master is spending most of its time waiting for something to happen.




## For More Information

For the latest information about Work Queue, please visit our [web
site](http://ccl.cse.nd.edu/software/workqueue) and subscribe to our [mailing
list](http://ccl.cse.nd.edu/software/help.shtml).

Work Queue is Copyright (C) 2016- The University of Notre Dame. All rights
reserved. This software is distributed under the GNU General Public License.
See the file COPYING for details.

Last modified: August 2019

