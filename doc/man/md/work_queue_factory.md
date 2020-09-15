






















# work_queue_factory(1)

## NAME
**work_queue_factory** - maintain a pool of Work Queue workers on a batch system.

## SYNOPSIS
****work_queue_factory -M <project-name> -T <batch-type> [options]****

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

### Batch Options

- **-M --manager-name <project>** Project name of managers to serve, can be a regular expression.
- **-F --foremen-name <project>** Foremen to serve, can be a regular expression.
- **--catalog catalog** Catalog server to query for managers (default: catalog.cse.nd.edu,backup-catalog.cse.nd.edu:9097).
- **-T --batch-type <type>** Batch system type (required). One of: local, wq, condor, sge, torque, mesos, k8s, moab, slurm, chirp, amazon, lambda, dryrun, amazon-batch
- **-B --batch-options <options>** Add these options to all batch submit files.
- **-P --password <file>** Password file for workers to authenticate to manager.
- **-C --config-file <file>** Use the configuration file <file>.
- **-w --min-workers <workers>** Minimum workers running.  (default=5)
- **-W --max-workers <workers>** Maximum workers running.  (default=100)
- **--workers-per-cycle workers** Maximum number of new workers per 30 seconds.  ( less than 1 disables limit, default=5)
- **--tasks-per-worker workers** Average tasks per worker (default=one task per core).
- **-t --timeout <time>** Workers abort after this amount of idle time (default=300).
- **--env variable=value** Environment variable that should be added to the worker (May be specified multiple times).
- **-E --extra-options <options>** Extra options that should be added to the worker.
- **--cores n** Set the number of cores requested per worker.
- **--gpus n** Set the number of GPUs requested per worker.
- **--memory mb** Set the amount of memory (in MB) requested per worker.
- **--disk mb** Set the amount of disk (in MB) requested per worker.
- **--autosize** Automatically size a worker to an available slot (Condor, Mesos, and Kubernetes).
- **--condor-requirements** Set requirements for the workers as Condor jobs. May be specified several times with expresions and-ed together (Condor only).
- **--factory-timeout n** Exit after no manager has been seen in <n> seconds.
- **-S --scratch-dir <file>** Use this scratch dir for temporary files (default is /tmp/wq-pool-$uid).
- **-c, --capacity** Use worker capacity reported by managers.
- **-d --debug <subsystem>** Enable debugging for this subsystem.
- **--amazon-config** Specify Amazon config file (for use with -T amazon).
- **--wrapper** Wrap factory with this command prefix.
- **--wrapper-input** Add this input file needed by the wrapper.
- **--mesos-manager hostname** Specify the host name to mesos manager node (for use with -T mesos).
- **--mesos-path filepath** Specify path to mesos python library (for use with -T mesos).
- **--mesos-preload library** Specify the linking libraries for running mesos (for use with -T mesos).
- **--k8s-image** Specify the container image for using Kubernetes (for use with -T k8s).
- **--k8s-worker-image** Specify the container image that contains work_queue_worker availabe for using Kubernetes (for use with -T k8s).
- **-o --debug-file <file>** Send debugging to this file (can also be :stderr, or :stdout).
- **-O --debug-file-size <mb>** Specify the size of the debug file (must use with -o option).
- **--worker-binary file** Specify the binary to use for the worker (relative or hard path). It should accept the same arguments as the default work_queue_worker.
- **--runos img** Will make a best attempt to ensure the worker will execute in the specified OS environment, regardless of the underlying OS.
- **--run-factory-as-manager** Force factory to run itself as a work queue manager.
- **-v, --version** Show the version string.
- **-h, --help** Show this screen.


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
The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 8.0.0 DEVELOPMENT released on 
