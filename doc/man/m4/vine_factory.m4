include(manual.h)dnl
HEADER(vine_factory)

SECTION(NAME)
BOLD(vine_factory) - maintain a pool of TaskVine workers on a batch system.

SECTION(SYNOPSIS)
CODE(vine_factory -M PARAM(project-name) -T PARAM(batch-type) [options])

SECTION(DESCRIPTION)
BOLD(vine_factory) submits and maintains a number
of MANPAGE(vine_worker,1) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a BOLD(vine_factory) process
will be directed to work for a specific manager, or any set of managers matching
a given project name.  BOLD(vine_factory) will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.
PARA
By default, BOLD(vine_factory) will run as many workers as the
indicated managers have tasks ready to run.  If there are multiple
managers, then enough workers will be started to satisfy their collective needs.
For example, if there are two managers with the same project name, each with
10 tasks to run, then BOLD(vine_factory) will start a total of 20 workers.
PARA
If the number of needed workers increases, BOLD(vine_factory) will submit
more workers to meet the desired need.  However, it will not run more than
a fixed maximum number of workers, given by the -W option.
PARA
If the need for workers drops, BOLD(vine_factory) does not remove them immediately,
but waits to them to exit on their own.  (This happens when the worker has been idle
for a certain time.)  A minimum number of workers will be maintained, given
by the -w option.
PARA
If given the -c option, then BOLD(vine_factory) will consider the capacity
reported by each manager.  The capacity is the estimated number of workers
that the manager thinks it can handle, based on the task execution and data
transfer times currently observed at the manager.  With the -c option on,
BOLD(vine_factory) will consider the manager's capacity to be the maximum
number of workers to run.
PARA
If BOLD(vine_factory) receives a terminating signal, it will attempt to
remove all running workers before exiting.

SECTION(OPTIONS)

General options:

OPTIONS_BEGIN
OPTION_ARG(T,batch-type,type) Batch system type (required). One of: local, wq, condor, sge, pbs, lsf, torque, moab, mpi, slurm, chirp, amazon, amazon-batch, lambda, mesos, k8s, dryrun
OPTION_ARG(C,config-file,file) Use configuration file PARAM(file).
OPTION_ARG(M,manager-name,project) Project name of managers to server, can be regex
OPTION_ARG(F,foremen-name,project) Foremen to serve, can be a regular expression.
OPTION_ARG_LONG(catalog,host:port) Catalog server to query for managers.
OPTION_ARG(P,password,pwdfile) Password file for workers to authenticate.
OPTION_ARG(S,scratch-dir,dir) Use this scratch dir for factory. Default is /tmp/wq-factory-$UID. 
Also configurable through environment variables BOLD(CCTOOLS_TEMP) or BOLD(TMPDIR)
OPTION_FLAG_LONG(run-factory-as-manager) Force factory to run itself as a manager.
OPTION_FLAG_LONG(parent-death) Exit if parent process dies.
OPTION_ARG(d,debug,subsystem) Enable debugging for this subsystem.
OPTION_ARG(o,debug-file,file) Send debugging to this file.
OPTION_ARG(O,debug-file-size,mb) Specify the size of the debug file.
OPTION_FLAG(v,version) Show the version string.
OPTION_FLAG(h,help) Show this screen.
OPTIONS_END

Concurrent control options:

OPTIONS_BEGIN
OPTION_ARG(w,min-workers,n) Minimum workers running (default=5).
OPTION_ARG(W,max-workers,n) Maximum workers running (default=100).
OPTION_ARG_LONG(workers-per-cycle,n) Max number of new workers per 30s (default=5)
OPTION_ARG(t,timeout,time) Workers abort after idle time (default=300).
OPTION_ARG_LONG(factory-timeout,n) Exit after no manager seen in PARAM(n) seconds.
OPTION_ARG_LONG(tasks-per-worker,n) Average tasks per worker (default=one per core).
OPTION_ARG(c,capacity,cap) Use worker capacity reported by managers.
OPTIONS_END

Resource management options:
OPTIONS_BEGIN
OPTION_ARG_LONG(cores,n)
 Set the number of cores requested per worker.
OPTION_ARG_LONG(gpus,n)
 Set the number of GPUs requested per worker.
OPTION_ARG_LONG(memory,mb)
 Set the amount of memory (in MB) per worker.
OPTION_ARG_LONG(disk,mb)
 Set the amount of disk (in MB) per worker.
OPTION_FLAG_LONG(autosize)
 Autosize worker to slot (Condor, Mesos, K8S).
OPTIONS_END

Worker environment options:
OPTIONS_BEGIN
OPTION_ARG_LONG(env,variable=value)
 Environment variable to add to worker.
OPTION_ARG(E,extra-options,options)
 Extra options to give to worker.
OPTION_ARG_LONG(worker-binary,file)
 Alternate binary instead of vine_worker.
OPTION_ARG_LONG(wrapper,cmd)
 Wrap factory with this command prefix.
OPTION_ARG_LONG(wrapper-input,file) Add this input file needed by the wrapper.
OPTION_ARG_LONG(python-env,file.tar.gz) Run each worker inside this python environment.
OPTIONS_END

Options  specific to batch systems:
OPTIONS_BEGIN
OPTION_ARG(B,batch-options,options) Generic batch system options.
OPTION_ARG_LONG(amazon-config,cfg) Specify Amazon config file.
OPTION_ARG_LONG(condor-requirements,reqs) Set requirements for the workers as Condor jobs.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose you have a TaskVine manager with a project name of "barney".
To maintain workers for barney, do this:

LONGCODE_BEGIN
vine_factory -T condor -M barney
LONGCODE_END

To maintain a maximum of 100 workers on an SGE batch system, do this:

LONGCODE_BEGIN
vine_factory -T sge -M barney -W 100
LONGCODE_END

To start workers such that the workers exit after 5 minutes (300s) of idleness:

LONGCODE_BEGIN
vine_factory -T condor -M barney -t 300
LONGCODE_END

If you want to start workers that match any project that begins
with barney, use a regular expression:

LONGCODE_BEGIN
vine_factory -T condor -M "barney.*" -t 300
LONGCODE_END

If running on condor, you may manually specify condor requirements:

LONGCODE_BEGIN
vine_factory -T condor -M barney --condor-requirements 'MachineGroup == "disc"' --condor-requirements 'has_matlab == true'
LONGCODE_END

Repeated uses of CODE(condor-requirements) are and-ed together. The previous example will produce a statement equivalent to:

CODE(requirements = ((MachineGroup == "disc") && (has_matlab == true)))

Use the configuration file BOLD(my_conf):

LONGCODE_BEGIN
vine_factory -Cmy_conf
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

SEE_ALSO_TASK_VINE

FOOTER
