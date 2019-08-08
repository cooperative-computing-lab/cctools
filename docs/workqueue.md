# Work Queue User's Manual

Work Queue is Copyright (C) 2016- The University of Notre Dame. All rights
reserved. This software is distributed under the GNU General Public License.
See the file COPYING for details.

## Overview⇗

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

## Installing Work Queue⇗

Work Queue is part of the [Cooperating Computing
Tools](http://ccl.cse.nd.edu/software). The CCTools package can be downloaded
from [this web page](http://ccl.cse.nd.edu/software/download). Follow the
[installation instructions](install.html) to setup CCTools required for
running Work Queue. The documentation for the full set of features of the Work
Queue API can be viewed from either within the CCTools package or
[here](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html)
and
[here](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceWorkQueuePython.html).

## Building a Work Queue Application⇗

Let's begin by running a simple but complete example of a Work Queue
application. After trying it out, we will then show how to write a Work Queue
application from scratch.

We assume that you have downloaded and installed the cctools package in your
home directory under `$HOME/cctools`. Next, download the example file for the
language of your choice:

  * C: [work_queue_example.c](work_queue_example.c)
  * Python: [work_queue_example.py](work_queue_example.py)
  * Perl: [work_queue_example.pl](work_queue_example.pl)

If you are using the C example, compile it like this:

    
    
    gcc work_queue_example.c -o work_queue_example -I${HOME}/cctools/include/cctools -L${HOME}/cctools/lib -lwork_queue -ldttools -lm -lz
    

If you are using the Python example, set PYTHONPATH to include the Python
modules in cctools: (You may need to adjust the version number to match your
Python.)

    
    
    export PYTHONPATH=${PYTHONPATH}:${HOME}/cctools/lib/python2.7/site-packages
    

If you are using the Perl example, set PERL5LIB to include the Perl modules in
cctools:

    
    
    export PERL5LIB=${PERL5LIB}:${HOME}/cctools/lib/perl5/site_perl
    

## Running a Work Queue Application⇗

The example application simply compresses a bunch of files in parallel. The
files to be compressed must be listed on the command line. Each will be
transmitted to a remote worker, compressed, and then sent back to the Work
Queue master. To compress files `a`, `b`, and `c` with this example
application, run it as:

    
    
    ./work_queue_example a b c
    

You will see this right away:

    
    
    listening on port 9123...
    submitted task: /usr/bin/gzip < a > a.gz
    submitted task: /usr/bin/gzip < b > b.gz
    submitted task: /usr/bin/gzip < c > c.gz
    waiting for tasks to complete...
    

The Work Queue master is now waiting for workers to connect and begin
requesting work. (Without any workers, it will wait forever.) You can start
one worker on the same machine by opening a new shell and running:

    
    
    work_queue_worker MACHINENAME 9123
    

(Obviously, substitute the name of your machine for MACHINENAME.) If you have
access to other machines, you can `ssh` there and run workers as well. In
general, the more you start, the faster the work gets done. If a worker should
fail, the work queue infrastructure will retry the work elsewhere, so it is
safe to submit many workers to an unreliable system.

If you have access to a Condor pool, you can use this shortcut to submit ten
workers at once via Condor:

    
    
    % condor_submit_workers MACHINENAME 9123 10
    Submitting job(s)..........
    Logging submit event(s)..........
    10 job(s) submitted to cluster 298.
    

Or, if you have access to an SGE cluster, do this:

    
    
    % sge_submit_workers MACHINENAME 9123 10
    Your job 153083 ("worker.sh") has been submitted
    Your job 153084 ("worker.sh") has been submitted
    Your job 153085 ("worker.sh") has been submitted
    ...
    

Similar scripts are available for other common batch systems:

    
    
    pbs_submit_workers
    torque_submit_workers
    slurm_submit_workers
    ec2_submit_workers
    

When the master completes, if the workers were not shut down in the master,
your workers will still be available, so you can either run another master
with the same workers, or you can remove the workers with `kill`, `condor_rm`,
or `qdel` as appropriate. If you forget to remove them, they will exit
automatically after fifteen minutes. (This can be adjusted with the `-t`
option to `worker`.)

## Writing a Work Queue Master Program⇗

To write your own program using Work Queue, begin with [C
example](work_queue_example.c) or [Python example](work_queue_example.py) or
[Perl example](work_queue_example.pl) as a starting point. Here is a basic
outline for a Work Queue master:

    
    
    q = work_queue_create(port);
    
        for(all tasks) {
             t = work_queue_task_create(command);
             /* add to the task description */
             work_queue_submit(q,t);
        }
    
        while(!work_queue_empty(q)) {
            t = work_queue_wait(q);
            work_queue_task_delete(t);
        }
    
    work_queue_delete(q);
    

First create a queue that is listening on a particular TCP port:

#### C/Perl

    
    
     q = work_queue_create(port);
    

#### Python

    
    
     q = WorkQueue(port)
    

The master then creates tasks to submit to the queue. Each task consists of a
command line to run and a statement of what data is needed, and what data will
be produced by the command. Input data can be provided in the form of a file
or a local memory buffer. Output data can be provided in the form of a file or
the standard output of the program. It is also required to specify whether the
data, input or output, need to be cached at the worker site for later use.

In the example, we specify a command that takes a single input file and
produces a single output file. We then create a task by providing the
specified command as an argument:

#### C/Perl

    
    
     t = work_queue_task_create(command);
    

#### Python

    
    
     t = Task(command)
    

The input and output files associated with the execution of the task must be
explicitly specified. In the example, we also specify the executable in the
command invocation as an input file so that it is transferred and installed in
the working directory of the worker. We require this executable to be cached
so that it can be used by subsequent tasks that need it in their execution. On
the other hand, the input and output of the task are not required to be cached
since they are not used by subsequent tasks in this example.

#### C/Perl

    
    
     work_queue_task_specify_file(t,"/usr/bin/gzip","gzip",WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
     work_queue_task_specify_file(t,infile,infile,WORK_QUEUE_INPUT,WORK_QUEUE_NOCACHE);
     work_queue_task_specify_file(t,outfile,outfile,WORK_QUEUE_OUTPUT,WORK_QUEUE_NOCACHE);
    

#### Python

    
    
     t.specify_file("/usr/bin/gzip","gzip",WORK_QUEUE_INPUT,cache=True);
     t.specify_file(infile,infile,WORK_QUEUE_INPUT,cache=False)
     t.specify_file(outfile,outfile,WORK_QUEUE_OUTPUT,cache=False)
    

Note that the specified input directories and files for each task are
transferred and setup in the sandbox directory of the worker (unless an
absolute path is specified for their location). This sandbox serves as the
initial working directory of each task executed by the worker. The task
outputs are also stored in the sandbox directory (unless an absolute path is
specified for their storage). The path of the sandbox directory is exported to
the execution environment of each worker through the WORK_QUEUE_SANDBOX shell
environment variable. This shell variable can be used in the execution
environment of the worker to describe and access the locations of files in the
sandbox directory. An example of its usage is given below:

#### C/Perl

    
    
     t = work_queue_task_create("$WORK_QUEUE_SANDBOX/gzip < a > a.gz");
    

#### Python

    
    
     t = Task("$WORK_QUEUE_SANDBOX/gzip < a > a.gz")
    

We can also run a program that is already installed at the remote site, where
the worker runs, by specifying its installed location in the command line of
the task (and removing the specification of the executable as an input file).
For example:

#### C/Perl

    
    
     t = work_queue_task_create("/usr/bin/gzip < a > a.gz");
    

#### Python

    
    
     t = Task("/usr/bin/gzip < a > a.gz")
    

Once a task has been fully specified, it can be submitted to the queue where
it gets assigned a unique taskid:

#### C/Perl

    
    
     taskid = work_queue_submit(q,t);
    

#### Python

    
    
     taskid = q.submit(t)
    

Next, wait for a task to complete, stating how long you are willing to wait
for a result, in seconds. (If no tasks have completed by the timeout,
`work_queue_wait` will return null.)

#### C/Perl

    
    
     t = work_queue_wait(q,5);
    

#### Python

    
    
     t = q.wait(5)
    

A completed task will have its output files written to disk. You may examine
the standard output of the task in `t->output` and the exit code in
`t->exit_status`. When you are done with the task, delete it:

#### C/Perl

    
    
     work_queue_task_delete(t);
    

#### Python

    
    
     Deleted automatically when task object goes out of scope
    

Continue submitting and waiting for tasks until all work is complete. You may
check to make sure that the queue is empty with `work_queue_empty`. When all
is done, delete the queue:

#### C/Perl

    
    
     work_queue_delete(q);
    

#### Python

    
    
     Deleted automatically when work_queue object goes out of scope
    

Full details of all of the Work Queue functions can be found in the [Work
Queue
API](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html).

## Project Names and the Catalog Server⇗

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
specified to run in the ` WORK_QUEUE_MASTER_MODE_CATALOG`. See [Catalog
Servers](catalog.html) for details on specifying catalog servers.

For example, to have a Work Queue master advertise its project name as
`myproject`, add the following code snippet after creating the queue:

#### C/Perl

    
    
     work_queue_specify_master_mode(q, WORK_QUEUE_MASTER_MODE_CATALOG)
     work_queue_specify_name(q, "myproject");
    

#### Python

    
    
     wq.specify_mode(WORK_QUEUE_MASTER_MODE_CATALOG)
     wq.specify_name("myproject")
    

To start a worker for this master, specify the project name (`myproject`) to
connect in the `-N` option:

    
    
    work_queue_worker -N myproject
    

You can start ten workers for this master on Condor using
`condor_submit_workers` by providing the same option arguments.:

    
    
    % condor_submit_workers -N myproject 10
    Submitting job(s)..........
    Logging submit event(s)..........
    10 job(s) submitted to cluster 298.
    

Or similarly on SGE using `sge_submit_workers` as:

    
    
    % sge_submit_workers -N myproject 10
    Your job 153097 ("worker.sh") has been submitted
    Your job 153098 ("worker.sh") has been submitted
    Your job 153099 ("worker.sh") has been submitted
    ...
    

## Running Multiple Tasks per Worker⇗

Unless otherwise specified, Work Queue assumes that a single task runs on a
single worker at a time, and a single worker occupies an entire machine.

However, if you have large multi-core machines and multi-threaded tasks, you
will want one worker to manage multiple tasks running on a machine. For
example, if you have a 8-core machine, then you might want to run four 2-core
tasks on a single worker at once, being careful not to exceed the available
memory and disk.

Two steps are needed to make this happen. First, adjust your workers to manage
multiple cores at once. You can specify the exact number of cores to use like
this:

    
    
    % work_queue_worker --cores 8  MACHINENAME 9123
    

To limit cores, memory and disk, do this:

    
    
    % work_queue_worker --cores 8 --memory 1000 --disk 8000  MACHINENAME 9123
    

Second, you must annotate every task in the worker with resource requirements
in terms of cores, memory, and disk.

#### C/Perl

    
    
     work_queue_task_specify_cores(t, 2); //needs 2 cores
     work_queue_task_specify_memory(t, 100); //needs 100 MB memory
     work_queue_task_specify_disk(t, 1000); //needs 1 GB disk
    
    

#### Python

    
    
     t.specify_cores(2) #needs 2 cores
     t.specify_memory(100) #needs 100 MB memory
     t.specify_disk(1000) #needs 1 GB disk
    

Note that if no requirements are specified, a task consumes an entire worker.
All resource requirements must be specified in order to run multiple tasks on
a single worker. For example, if you annotate a task as using 1 core, but
don't specify its memory or disk requirments, Work Queue will only schedule
one task to a two-core worker. However, if you annotate the core, memory, and
disc requirements for a task, Work Queue can schedule two such tasks to a two-
task worker, assuming it has the available memory and disk requirements for
each individual task.

You may also use the --cores, --memory, and --disk options when using batch
submission scripts such as `condor_submit_workers` or `slurm_submit_workers`,
and the script will correctly ask the batch system for an appropiate node.

The only caveat is when using `sge_submit_workers`, as there are many
differences across systems that the script cannot manage. For `
sge_submit_workers ` you have to specify **both** the resources used by the
worker (i.e., with --cores, etc.) and the appropiate computing node with the `
-p ` option.

For example, say that your local SGE installation requires you to specify the
number of cores with the switch ` -pe smp ` , and you want workers with 4
cores:

    
    
    % sge_submit_workers --cores 4 -p "-pe smp 4" MACHINENAME 9123
    

If you find that there are options that are needed everytime, you can compile
CCTools using the ` --sge-parameter `. For example, at Notre Dame we
automatically set the number of cores as follows:

    
    
    % ./configure  --sge-parameter '-pe smp $cores'
    

So that we can simply call:

    
    
    % sge_submit_workers --cores 4 MACHINENAME 9123
    

The variables ` $cores `, ` $memory `, and ` $disk `, have the values of the
options passed to `--cores`, `--memory`, `--disk. `

## Scaling Up with Foremen⇗

A Work Queue foreman allows Work Queue workers to be managed in an
hierarchical manner. Each foreman connects to the Work Queue master and
accepts tasks as though it were a worker. It then accepts connections from
Work Queue workers and dispatches tasks to them as if it were the master.

A setup using foremen is beneficial when there are common files that need to
be transmitted to workers and cached for subsequent executions. In this case,
the foremen transfer the common files to their workers without requiring any
intervention from the master, thereby lowering the communication and transfer
overheads at the master.

Foremen are also useful when harnessing resources from multiple clusters. A
foreman can be run on the head node of a cluster acting as a single
communications hub for the workers in that cluster. This reduces the network
connections leaving the cluster and minimizes the transfer costs for sending
data into the cluster over wide area networks.

To start a Work Queue foreman, invoke `work_queue_worker` with the `--foreman`
option. The foreman can advertise a project name using the `-f,--foreman-name`
option to enable workers to find and connect to it without being given its
hostname and port. On the other end, the foreman will connect to the master
with the same project name specified in `-M` argument (alternatively, the
hostname and port of the master can be provided instead of its project name).

For example, to run a foreman that works for a master with project name
`myproject` and advertises itself as `foreman_myproject`:

    
    
    % work_queue_worker -f foreman_myproject -M myproject
    

To run a worker that connects to a foreman, specify the foreman's project name
in the `-N` option. For example:

    
    
    % work_queue_worker -N foreman_myproject
    

## Recommended Practices⇗

### Security⇗

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

#### C/Perl

    
    
     work_queue_specify_password_file(q,mypwfile);
    

#### Python

    
    
     q.specify_password_file(mypwfile)
    

And give the `--password` option to give the same password file to your
workers:

    
    
    work_queue_worker --password mypwfile  MACHINENAME 9123
    

With this option enabled, both the master and the workers will verify that the
other has the matching password before proceeding. The password is not sent in
the clear, but is securely verified through a SHA1-based challenge-response
protocol.

### Debugging⇗

Work Queue can be set up to print debug messages at the master and worker to
help troubleshoot failures, bugs, and errors.

When using the [C
API](http://www.nd.edu/~ccl/software/manuals/api/html/work__queue_8h.html)
include the
[debug.h](http://ccl.cse.nd.edu/software/manuals/api/html/debug_8h.html)
header to enable the debug messages at the master:

#### C

    
    
     #include <debug.h>
     cctools_debug_flags_set("all");
    

In Perl and Python, simply do:

#### Perl

    
    
     cctools_debug_flags_set("all");
    

#### Python

    
    
     cctools_debug_flags_set("all")
    

The `all` flag causes debug messages from every subsystem called by Work Queue
to be printed. More information about the debug flags are
[here](http://ccl.cse.nd.edu/software/manuals/api/html/debug_8h.html).

You can also redirect the debug messages to a file:

#### C/Perl

    
    
     cctools_debug_config_file("wq.debug");
    

#### Python

    
    
     cctools_debug_config_file("wq.debug")
    

To enable debugging at the worker, set the `-d` option:

    
    
    work_queue_worker -d all MACHINENAME 9123
    

To redirect the debug messages, specify the `-o` option:

    
    
    work_queue_worker -d all -o worker.debug MACHINENAME 9123
    

### Logging and Plotting

You can specify a log file to obtain a time series of work queue statistics.
Usually this log file is specified just after the creation of the queue as
follows:

#### C/Perl

    
    
     work_queue_specify_log(q, "mylogfile");
    
    

#### Python

    
    
     q.specify_log("mylogfile")
    
    

The script `work_queue_graph_log` is a wrapper for `gnuplot`, and with it you
can plot some of the statistics, such as total time spent transfering tasks,
number of tasks running, and workers connected:

    
    
     % work_queue_graph_log -o myplots mylogfile
     % ls
     % ... myplots.tasks.png myplots.tasks-log.png myplots.time.png myplots.time-log.png ...
    
    

We find it very helpful to plot these statistics when diagnosing a problem
with work queue applications.

### Standard Output Limits

The output printed by a task to stdout can be accessed in the `output` buffer
in [work_queue_task struct](api/html/structwork__queue__task.html). The size
of `output` is limited to 1 GB. Any output beyond 1 GB will be truncated. So,
please redirect the stdout of the task to a file and specify the file as an
output file of the task using `work_queue_task_specify_file` (`specify_file`
in Python) as described above.

## Advanced Topics⇗

A variety of advanced features are available for programs with unusual needs
or very large scales. Each feature is described briefly here, and more details
may be found in the [Work Queue
API](http://ccl.cse.nd.edu/software/manuals/api/html/work__queue_8h.html).

### Pipelined Submission.

If you have a **very** large number of tasks to run, it may not be possible to
submit all of the tasks, and then wait for all of them. Instead, submit a
small number of tasks, then alternate waiting and submiting to keep a constant
number in the queue. ` work_queue_hungry` will tell you if more submission are
warranted.

### Watching Output Files

If you would like to see the output of a task as it is produced, add
WORK_QUEUE_WATCH to the flags argument of `work_queue_specify_file`. This will
cause the worker to periodically send output appended to that file back to the
master. This is useful for a program that produces a log or progress bar as
part of its output.

### Asynchronous transfer

If you have tasks with a balanced or large computation-to-data ratio, this
feature can help improve the CPU utilization and lower the runtime overheads
incurred due to data transfer. This feature asynchronously streams the data
inputs and outputs to and from the workers when they are executing tasks. See
`work_queue_specify_asynchrony`.

### Fast Abort

A large computation can often be slowed down by stragglers. If you have a
large number of small tasks that take a short amount of time, then Fast Abort
can help. The Fast Abort feature keeps statistics on tasks execution times and
proactively aborts tasks that are statistical outliers. See
`work_queue_activate_fast_abort`.

### Immediate Data

For a large number of tasks or workers, it may be impractical to create local
input files for each one. If the master already has the necessary input data
in memory, it can pass the data directly to the remote task with
`work_queue_task_specify_buffer`.

### String Interpolation

If you have workers distributed across multiple operating systems (such as
Linux, Cygwin, Solaris) and/or architectures (such as i686, x86_64) and have
files specific to each of these systems, this feature will help. The strings
$OS and $ARCH are available for use in the specification of input file names.
Work Queue will automatically resolve these strings to the operating system
and architecture of each connected worker and transfer the input file
corresponding to the resolved file name. For example:

#### C/Perl

    
    
     work_queue_task_specify_file(t,"a.$OS.$ARCH","a",WORK_QUEUE_INPUT,WORK_QUEUE_CACHE);
    

#### Python

    
    
     t.specify_file("a.$OS.$ARCH","a",WORK_QUEUE_INPUT,cache=True)
    

This will transfer `a.Linux.x86_64` to workers running on a Linux system with
an x86_64 architecture and `a.Cygwin.i686` to workers on Cygwin with an i686
architecture.

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

#### C/Perl

    
    
     t = work_queue_cancel_by_tasktag(q,"task3");
    

#### Python

    
    
     t = q.cancel_by_tasktag("task3")
    

This cancels a task with `tag` named 'task3'. Note that in the presence of
tasks with the same tag, `work_queue_cancel_by_tasktag` will cancel and
retrieve only one of the matching tasks.

### Worker Blacklist

You may find that certain hosts are not correctly configured to run your
tasks. The master can be directed to ignore certain workers with the blacklist
feature. For example:

#### C

    
    
     t = work_queue_wait(q, SECONDS);
     //if t fails given a worker misconfiguration:
     work_queue_blacklist_add(q, t->hostname);
    

#### Python

    
    
     t = q.wait(SECONDS)
     # if t fails given a worker misconfiguration:
     q.blacklist(t.hostname)
    

#### Perl

    
    
     t = work_queue_wait(q, SECONDS);
     # if t fails given a worker misconfiguration:
     work_queue_blacklist_add(q, t->{hostname});
    

### Performance Statistics

The queue tracks a fair number of statistics that count the number of tasks,
number of workers, number of failures, and so forth. Obtain this data with
`work_queue_get_stats` in order to make a progress bar or other user-visible
information.

## For More Information⇗

For the latest information about Work Queue, please visit our [web
site](http://ccl.cse.nd.edu/software/workqueue) and subscribe to our [mailing
list](http://ccl.cse.nd.edu/software/help.shtml).

