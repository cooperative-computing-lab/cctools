






















# work_queue_factory(1)

## NAME
**work_queue_factory** - maintain a pool of Work Queue workers on a batch system.

## SYNOPSIS
**work_queue_factory -M _&lt;project-name&gt;_ -T _&lt;batch-type&gt;_ [options]**

## DESCRIPTION
**work_queue_factory** submits and maintains a number
of [work_queue_worker(1)](work_queue_worker.md) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a **work_queue_factory** process
will be directed to work for a specific manager, or any set of managers matching
a given project name.  **work_queue_factory** will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.

By default, **work_queue_factory** will run as many workers as the
indicated managers have tasks ready to run.  If there are multiple
managers, then enough workers will be started to satisfy their collective needs.
For example, if there are two managers with the same project name, each with
10 tasks to run, then **work_queue_factory** will start a total of 20 workers.

If the number of needed workers increases, **work_queue_factory** will submit
more workers to meet the desired need.  However, it will not run more than
a fixed maximum number of workers, given by the -W option.

If the need for workers drops, **work_queue_factory** does not remove them immediately,
but waits to them to exit on their own.  (This happens when the worker has been idle
for a certain time.)  A minimum number of workers will be maintained, given
by the -w option.

If given the -c option, then **work_queue_factory** will consider the capacity
reported by each manager.  The capacity is the estimated number of workers
that the manager thinks it can handle, based on the task execution and data
transfer times currently observed at the manager.  With the -c option on,
**work_queue_factory** will consider the manager's capacity to be the maximum
number of workers to run.

If **work_queue_factory** receives a terminating signal, it will attempt to
remove all running workers before exiting.

## OPTIONS

General options:


- **-T**,**--batch-type=_&lt;type&gt;_**<br /> Batch system type (required). One of: local, wq, condor, sge, pbs, lsf, torque, moab, mpi, slurm, chirp, amazon, amazon-batch, lambda, mesos, k8s, dryrun
- **-C**,**--config-file=_&lt;file&gt;_**<br /> Use configuration file _&lt;file&gt;_.
- **-C**,**--config-file=_&lt;file&gt;_**<br /> Use configuration file _&lt;file&gt;_.
- **-M**,**--manager-name=_&lt;project&gt;_**<br /> Project name of managers to server, can be regex
- **-F**,**--foremen-name=_&lt;project&gt;_**<br /> Foremen to serve, can be a regular expression.
- **--catalog=_&lt;host:port&gt;_**<br /> Catalog server to query for managers.
- **-P**,**--password=_&lt;pwdfile&gt;_**<br /> Password file for workers to authenticate.
- **-S**,**--scratch-dir=_&lt;dir&gt;_**<br /> Use this scratch dir for factory. Default is /tmp/wq-factory-$UID.
- **--run-factory-as-manager**<br /> Force factory to run itself as a manager.
- **--parent-death**<br /> Exit if parent process dies.
- **-d**,**--debug=_&lt;subsystem&gt;_**<br /> Enable debugging for this subsystem.
- **-o**,**--debug-file=_&lt;file&gt;_**<br /> Send debugging to this file.
- **-O**,**--debug-file-size=_&lt;mb&gt;_**<br /> Specify the size of the debug file.
- **-v**,**--version**<br /> Show the version string.
- **-h**,**--help**<br /> Show this screen.


Concurrent control options:


- **-w**,**--min-workers=_&lt;n&gt;_**<br /> Minimum workers running (default=5).
- **-W**,**--max-workers=_&lt;n&gt;_**<br /> Maximum workers running (default=100).
- **--workers-per-cycle=_&lt;n&gt;_**<br /> Max number of new workers per 30s (default=5)
- **-t**,**--timeout=_&lt;time&gt;_**<br /> Workers abort after idle time (default=300).
- **--factory-timeout=_&lt;n&gt;_**<br /> Exit after no manager seen in _&lt;n&gt;_ seconds.
- **--tasks-per-worker=_&lt;n&gt;_**<br /> Average tasks per worker (default=one per core).
- **-c**,**--capacity=_&lt;cap&gt;_**<br /> Use worker capacity reported by managers.


Resource management options:

- **--cores=_&lt;n&gt;_**<br />
 Set the number of cores requested per worker.
- **--gpus=_&lt;n&gt;_**<br />
 Set the number of GPUs requested per worker.
- **--memory=_&lt;mb&gt;_**<br />
 Set the amount of memory (in MB) per worker.
- **--disk=_&lt;mb&gt;_**<br />
 Set the amount of disk (in MB) per worker.
- **--autosize**<br />
 Autosize worker to slot (Condor, Mesos, K8S).


Worker environment options:

- **--env=_&lt;variable=value&gt;_**<br />
 Environment variable to add to worker.
- **-E**,**--extra-options=_&lt;options&gt;_**<br />
 Extra options to give to worker.
- **--worker-binary=_&lt;file&gt;_**<br />
 Alternate binary instead of work_queue_worker.
- **--wrapper=_&lt;cmd&gt;_**<br />
 Wrap factory with this command prefix.
- **--wrapper-input=_&lt;file&gt;_**<br /> Add this input file needed by the wrapper.
- **--python-package=_&lt;pkg&gt;_**<br /> Run each worker inside this python package.


Options  specific to batch systems:

- **-B**,**--batch-options=_&lt;options&gt;_**<br /> Generic batch system options.
- **--amazon-config=_&lt;cfg&gt;_**<br /> Specify Amazon config file.
- **--condor-requirements=_&lt;reqs&gt;_**<br /> Set requirements for the workers as Condor jobs.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES

Suppose you have a Work Queue manager with a project name of "barney".
To maintain workers for barney, do this:

```
work_queue_factory -T condor -M barney
```

To maintain a maximum of 100 workers on an SGE batch system, do this:

```
work_queue_factory -T sge -M barney -W 100
```

To start workers such that the workers exit after 5 minutes (300s) of idleness:

```
work_queue_factory -T condor -M barney -t 300
```

If you want to start workers that match any project that begins
with barney, use a regular expression:

```
work_queue_factory -T condor -M barney.\* -t 300
```

If running on condor, you may manually specify condor requirements:

```
work_queue_factory -T condor -M barney --condor-requirements 'MachineGroup == "disc"' --condor-requirements 'has_matlab == true'
```

Repeated uses of **condor-requirements** are and-ed together. The previous example will produce a statement equivalent to:

**requirements = ((MachineGroup == "disc") && (has_matlab == true))**

Use the configuration file **my_conf**:

```
work_queue_factory -Cmy_conf
```

**my_conf** should be a proper JSON document, as:
```
{
        "manager-name": "my_manager.*",
        "max-workers": 100,
        "min-workers": 0
}
```

Valid configuration fields are:

```
manager-name
foremen-name
min-workers
max-workers
workers-per-cycle
task-per-worker
timeout
worker-extra-options
condor-requirements
cores
memory
disk
```

## KNOWN BUGS

The capacity measurement currently assumes single-core tasks running on single-core
workers, and behaves unexpectedly with multi-core tasks or multi-core workers.

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 8.0.0 DEVELOPMENT
