include(manual.h)dnl
HEADER(work_queue_factory)dnl

SECTION(NAME)
BOLD(work_queue_factory) - maintain a pool of Work Queue workers on a batch system.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_factory -M PARAM(project-name) -T PARAM(batch-type) [options]))

SECTION(DESCRIPTION)
BOLD(work_queue_factory) submits and maintains a number
of MANPAGE(work_queue_worker,1) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a BOLD(work_queue_factory) process
will be directed to work for a specific master, or any set of masters matching
a given project name.  BOLD(work_queue_factory) will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.
PARA
By default, BOLD(work_queue_factory) will run as many workers as the
indicated masters have tasks ready to run.  If there are multiple
masters, then enough workers will be started to satisfy their collective needs.
For example, if there are two masters with the same project name, each with
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
reported by each master.  The capacity is the estimated number of workers
that the master thinks it can handle, based on the task execution and data
transfer times currently observed at the master.  With the -c option on,
BOLD(work_queue_factory) will consider the master's capacity to be the maximum
number of workers to run.
PARA
If BOLD(work_queue_factory) receives a terminating signal, it will attempt to
remove all running workers before exiting.

SECTION(OPTIONS)

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-M,master-name, project)Name of a preferred project. A worker can have multiple preferred projects.
OPTION_TRIPLET(-T,batch-type, type)Batch system type: local, condor, sge, pbs, torque, blue_waters, slurm, moab, cluster, amazon. (default is local)
OPTION_TRIPLET(-B,batch-options, options)Add these options to all batch submit files.
OPTION_TRIPLET(-w,min-workers,workers) Minimum workers running.  (default=5)
OPTION_TRIPLET(-W,max-workers,workers) Maximum workers running.  (default=100)
OPTION_PAIR(--workers-per-cycle,workers) Maximum number of new workers per 30 seconds.  ( less than 1 disables limit, default=5)
OPTION_ITEM(`--autosize')Automatically size a worker to an available slot (Condor only).
OPTION_ITEM(-c --capacity) Use worker capacity reported by masters.
OPTION_TRIPLET(-P,password,file) Password file for workers to authenticate to master.
OPTION_TRIPLET(-t,timeout,time)Abort after this amount of idle time.
OPTION_TRIPLET(-C,config-file,file)Use the configuration file <file>.
OPTION_TRIPLET(-E,extra-options,options)Extra options that should be added to the worker.
OPTION_PAIR(--condor-requirements, str)Manually set requirements for the workers as condor jobs. May be specified several times, with the expresions and-ed together (Condor only).
OPTION_TRIPLET(-S,scratch,file)Scratch directory. (default is /tmp/${USER}-workers)
OPTION_PAIR(--factory-timeout, n)Exit after no master has been seen in <n> seconds.
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
OPTION_PAIR(--factory-timeout, #)Set factory timeout to <#> seconds. (off by default) This will cause work queue to exit when their are no masters present after the given number of seconds.
OPTION_PAIR(--wrapper,Wrap all commands with this prefix.)
OPTION_PAIR(--wrapper-input,Add this file needed by the wrapper.)
OPTION_ITEM(`-h, --help')Show this screen.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose you have a Work Queue master with a project name of "barney".
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
work_queue_factory -T condor -M barney --condor_requirements 'MachineGroup == "disc"' --condor_requirements 'has_matlab == true'
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
        "master-name": "my_master.*",
        "max-workers": 100,
        "min-workers": 0
}
LONGCODE_END

Valid configuration fields are:

LONGCODE_BEGIN
master-name
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
