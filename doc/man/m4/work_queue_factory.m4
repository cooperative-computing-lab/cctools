include(manual.h)dnl
HEADER(work_queue_factory)

SECTION(NAME)
BOLD(work_queue_factory) - maintain a pool of Work Queue workers on a batch system.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_factory -M PARAM(project-name) -T PARAM(batch-type) [options]))

SECTION(DESCRIPTION)
BOLD(work_queue_factory) submits and maintains a number
of MANPAGE(work_queue_worker,1) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a BOLD(work_queue_factory) process
will be directed to work for a specific manager, or any set of managers matching
a given project name.  BOLD(work_queue_factory) will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.
PARA
By default, BOLD(work_queue_factory) will run as many workers as the
indicated managers have tasks ready to run.  If there are multiple
managers, then enough workers will be started to satisfy their collective needs.
For example, if there are two managers with the same project name, each with
10 tasks to run, then BOLD(work_queue_factory) will start a total of 20 workers.
PARA
If the number of needed workers increases, BOLD(work_queue_factory) will submit
more workers to meet the desired need.  However, it will not run more than
a fixed maximum number of workers, given by the -W option.
PARA
If the need for workers drops, BOLD(work_queue_factory) does not remove them immediately,
but waits to them to exit on their own.  (This happens when the worker has been idle
for a certain time.)  A minimum number of workers will be maintained, given
by the -w option.
PARA
If given the -c option, then BOLD(work_queue_factory) will consider the capacity
reported by each manager.  The capacity is the estimated number of workers
that the manager thinks it can handle, based on the task execution and data
transfer times currently observed at the manager.  With the -c option on,
BOLD(work_queue_factory) will consider the manager's capacity to be the maximum
number of workers to run.
PARA
If BOLD(work_queue_factory) receives a terminating signal, it will attempt to
remove all running workers before exiting.

SECTION(OPTIONS)

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-M,manager-name,project)Project name of managers to serve, can be a regular expression.
OPTION_TRIPLET(-F,foremen-name,project)Foremen to serve, can be a regular expression.
OPTION_PAIR(--catalog, catalog)Catalog server to query for managers (default: catalog.cse.nd.edu,backup-catalog.cse.nd.edu:9097).
OPTION_TRIPLET(-T,batch-type,type)Batch system type (required). One of: local, wq, condor, sge, torque, mesos, k8s, moab, slurm, chirp, amazon, lambda, dryrun, amazon-batch
OPTION_TRIPLET(-B,batch-options,options)Add these options to all batch submit files.
OPTION_TRIPLET(-P,password,file)Password file for workers to authenticate to manager.
OPTION_TRIPLET(-C,config-file,file)Use the configuration file <file>.
OPTION_TRIPLET(-w,min-workers,workers)Minimum workers running.  (default=5)
OPTION_TRIPLET(-W,max-workers,workers)Maximum workers running.  (default=100)
OPTION_PAIR(--workers-per-cycle,workers)Maximum number of new workers per 30 seconds.  ( less than 1 disables limit, default=5)
OPTION_PAIR(--tasks-per-worker,workers)Average tasks per worker (default=one task per core).
OPTION_TRIPLET(-t,timeout,time)Workers abort after this amount of idle time (default=300).
OPTION_PAIR(--env,variable=value)Environment variable that should be added to the worker (May be specified multiple times).
OPTION_TRIPLET(-E,extra-options,options)Extra options that should be added to the worker.
OPTION_PAIR(--cores,n)Set the number of cores requested per worker.
OPTION_PAIR(--gpus,n)Set the number of GPUs requested per worker.
OPTION_PAIR(--memory,mb)Set the amount of memory (in MB) requested per worker.
OPTION_PAIR(--disk,mb)Set the amount of disk (in MB) requested per worker.
OPTION_ITEM(--autosize)Automatically size a worker to an available slot (Condor, Mesos, and Kubernetes).
OPTION_ITEM(--condor-requirements)Set requirements for the workers as Condor jobs. May be specified several times with expresions and-ed together (Condor only).
OPTION_PAIR(--factory-timeout,n)Exit after no manager has been seen in <n> seconds.
OPTION_TRIPLET(-S,scratch-dir,file)Use this scratch dir for temporary files (default is /tmp/wq-pool-$uid).
OPTION_ITEM(`-c, --capacity')Use worker capacity reported by managers.
OPTION_TRIPLET(-d,debug,subsystem)Enable debugging for this subsystem.
OPTION_ITEM(--amazon-config)Specify Amazon config file (for use with -T amazon).
OPTION_ITEM(--wrapper)Wrap factory with this command prefix.
OPTION_ITEM(--wrapper-input)Add this input file needed by the wrapper.
OPTION_PAIR(--mesos-manager,hostname)Specify the host name to mesos manager node (for use with -T mesos).
OPTION_PAIR(--mesos-path,filepath)Specify path to mesos python library (for use with -T mesos).
OPTION_PAIR(--mesos-preload,library)Specify the linking libraries for running mesos (for use with -T mesos).
OPTION_ITEM(--k8s-image)Specify the container image for using Kubernetes (for use with -T k8s).
OPTION_ITEM(--k8s-worker-image)Specify the container image that contains work_queue_worker availabe for using Kubernetes (for use with -T k8s).
OPTION_TRIPLET(-o,debug-file,file)Send debugging to this file (can also be :stderr, or :stdout).
OPTION_TRIPLET(-O,debug-file-size,mb)Specify the size of the debug file (must use with -o option).
OPTION_PAIR(--worker-binary,file)Specify the binary to use for the worker (relative or hard path). It should accept the same arguments as the default work_queue_worker.
OPTION_PAIR(--runos,img)Will make a best attempt to ensure the worker will execute in the specified OS environment, regardless of the underlying OS.
OPTION_ITEM(--run-factory-as-manager)Force factory to run itself as a work queue manager.
OPTION_ITEM(`-v, --version')Show the version string.
OPTION_ITEM(`-h, --help')Show this screen.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose you have a Work Queue manager with a project name of "barney".
To maintain workers for barney, do this:

LONGCODE_BEGIN
work_queue_factory -T condor -M barney
LONGCODE_END

To maintain a maximum of 100 workers on an SGE batch system, do this:

LONGCODE_BEGIN
work_queue_factory -T sge -M barney -W 100
LONGCODE_END

To start workers such that the workers exit after 5 minutes (300s) of idleness:

LONGCODE_BEGIN
work_queue_factory -T condor -M barney -t 300
LONGCODE_END

If you want to start workers that match any project that begins
with barney, use a regular expression:

LONGCODE_BEGIN
work_queue_factory -T condor -M barney.\* -t 300
LONGCODE_END

If running on condor, you may manually specify condor requirements:

LONGCODE_BEGIN
work_queue_factory -T condor -M barney --condor-requirements 'MachineGroup == "disc"' --condor-requirements 'has_matlab == true'
LONGCODE_END

Repeated uses of CODE(condor-requirements) are and-ed together. The previous example will produce a statement equivalent to:

CODE(requirements = ((MachineGroup == "disc") && (has_matlab == true)))

Use the configuration file BOLD(my_conf):

LONGCODE_BEGIN
work_queue_factory -Cmy_conf
LONGCODE_END

BOLD(my_conf) should be a proper JSON document, as:
LONGCODE_BEGIN
{
        "manager-name": "my_manager.*",
        "max-workers": 100,
        "min-workers": 0
}
LONGCODE_END

Valid configuration fields are:

LONGCODE_BEGIN
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
LONGCODE_END

SECTION(KNOWN BUGS)

The capacity measurement currently assumes single-core tasks running on single-core
workers, and behaves unexpectedly with multi-core tasks or multi-core workers.

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER
