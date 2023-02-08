






















# vine_factory(1)

## NAME
**vine_factory** - maintain a pool of TaskVine workers on a batch system.

## SYNOPSIS
**vine_factory -M _&lt;project-name&gt;_ -T _&lt;batch-type&gt;_ [options]**

## DESCRIPTION
**vine_factory** submits and maintains a number
of [vine_worker(1)](vine_worker.md) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a **vine_factory** process
will be directed to work for a specific manager, or any set of managers matching
a given project name.  **vine_factory** will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.

By default, **vine_factory** will run as many workers as the
indicated managers have tasks ready to run.  If there are multiple
managers, then enough workers will be started to satisfy their collective needs.
For example, if there are two managers with the same project name, each with
10 tasks to run, then **vine_factory** will start a total of 20 workers.

If the number of needed workers increases, **vine_factory** will submit
more workers to meet the desired need.  However, it will not run more than
a fixed maximum number of workers, given by the -W option.

If the need for workers drops, **vine_factory** does not remove them immediately,
but waits to them to exit on their own.  (This happens when the worker has been idle
for a certain time.)  A minimum number of workers will be maintained, given
by the -w option.

If given the -c option, then **vine_factory** will consider the capacity
reported by each manager.  The capacity is the estimated number of workers
that the manager thinks it can handle, based on the task execution and data
transfer times currently observed at the manager.  With the -c option on,
**vine_factory** will consider the manager's capacity to be the maximum
number of workers to run.

If **vine_factory** receives a terminating signal, it will attempt to
remove all running workers before exiting.

## OPTIONS

General options:


- **-T**,**--batch-type=_&lt;type&gt;_**<br /> Batch system type (required). One of: local, wq, condor, sge, pbs, lsf, torque, moab, mpi, slurm, chirp, amazon, amazon-batch, lambda, mesos, k8s, dryrun
- **-C**,**--config-file=_&lt;file&gt;_**<br /> Use configuration file _&lt;file&gt;_.
- **-M**,**--manager-name=_&lt;project&gt;_**<br /> Project name of managers to server, can be regex
- **-F**,**--foremen-name=_&lt;project&gt;_**<br /> Foremen to serve, can be a regular expression.
- **--catalog=_&lt;host:port&gt;_**<br /> Catalog server to query for managers.
- **-P**,**--password=_&lt;pwdfile&gt;_**<br /> Password file for workers to authenticate.
- **-S**,**--scratch-dir=_&lt;dir&gt;_**<br /> Use this scratch dir for factory. Default is /tmp/wq-factory-$UID. 
Also configurable through environment variables **CCTOOLS_TEMP** or **TMPDIR**
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
 Alternate binary instead of vine_worker.
- **--wrapper=_&lt;cmd&gt;_**<br />
 Wrap factory with this command prefix.
- **--wrapper-input=_&lt;file&gt;_**<br /> Add this input file needed by the wrapper.
- **--python-env=_&lt;file.tar.gz&gt;_**<br /> Run each worker inside this python environment.


Options  specific to batch systems:

- **-B**,**--batch-options=_&lt;options&gt;_**<br /> Generic batch system options.
- **--amazon-config=_&lt;cfg&gt;_**<br /> Specify Amazon config file.
- **--condor-requirements=_&lt;reqs&gt;_**<br /> Set requirements for the workers as Condor jobs.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES

Suppose you have a TaskVine manager with a project name of "barney".
To maintain workers for barney, do this:

```
vine_factory -T condor -M barney
```

To maintain a maximum of 100 workers on an SGE batch system, do this:

```
vine_factory -T sge -M barney -W 100
```

To start workers such that the workers exit after 5 minutes (300s) of idleness:

```
vine_factory -T condor -M barney -t 300
```

If you want to start workers that match any project that begins
with barney, use a regular expression:

```
vine_factory -T condor -M "barney.*" -t 300
```

If running on condor, you may manually specify condor requirements:

```
vine_factory -T condor -M barney --condor-requirements 'MachineGroup == "disc"' --condor-requirements 'has_matlab == true'
```

Repeated uses of **condor-requirements** are and-ed together. The previous example will produce a statement equivalent to:

**requirements = ((MachineGroup == "disc") && (has_matlab == true))**

Use the configuration file **my_conf**:

```
vine_factory -Cmy_conf
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
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [TaskVine User Manual]("../taskvine.html")
- [vine_worker(1)](vine_worker.md) [vine_status(1)](vine_status.md) [vine_factory(1)](vine_factory.md) [vine_graph_log(1)](vine_graph_log.md) 


CCTools
