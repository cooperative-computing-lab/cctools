include(manual.h)dnl
HEADER(work_queue_pool)dnl

SECTION(NAME)
BOLD(work_queue_pool) - maintain a pool of Work Queue workers on a batch system.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_pool -M PARAM(project-name) -T PARAM(batch-type) [options]))

SECTION(DESCRIPTION)
BOLD(work_queue_pool) submits and maintains a number
of MANPAGE(work_queue_worker,1) processes on various batch systems, such as
Condor and SGE.  All the workers managed by a BOLD(work_queue_pool) process
will be directed to work for a specific master, or any set of masters matching
a given project name.  BOLD(work_queue_pool) will automatically determine
the correct number of workers to have running, based on criteria set on
the command line.  The decision on how many workers to run is reconsidered
once per minute.
PARA
By default, BOLD(work_queue_pool) will run as many workers as the
indicated masters have tasks ready to run.  If there are multiple
masters, then enough workers will be started to satisfy their collective needs.
For example, if there are two masters with the same project name, each with
10 tasks to run, then BOLD(work_queue_pool) will start a total of 20 workers.
PARA
If the number of needed workers increases, BOLD(work_queue_pool) will submit
more workers to meet the desired need.  However, it will not run more than
a fixed maximum number of workers, given by the -W option.
PARA
If the need for workers drops, BOLD(work_queue_pool) does not remove them immediately,
but waits to them to exit on their own.  (This happens when the worker has been idle
for a certain time.)  A minimum number of workers will be maintained, given
by the -w option.
PARA
If given the -c option, then BOLD(work_queue_pool) will consider the capacity
reported by each master.  The capacity is the estimated number of workers
that the master thinks it can handle, based on the task execution and data
transfer times currently observed at the master.  With the -c option on,
BOLD(work_queue_pool) will consider the master's capacity to be the maximum
number of workers to run.
PARA
If BOLD(work_queue_pool) receives a terminating signal, it will attempt to
remove all running workers before exiting.

SECTION(OPTIONS)

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-M,master-name, project)Name of a preferred project. A worker can have multiple preferred projects.
OPTION_TRIPLET(-T,batch-type, type)Batch system type: unix, condor, sge, workqueue, xgrid. (default is unix)
OPTION_TRIPLET(-w,min-workers,workers) Minimum workers running.  (default=5)
OPTION_TRIPLET(-W,max-workers,workers) Maximum workers running.  (default=100)
OPTION_ITEM(-c --capacity) Use worker capacity reported by masters.
OPTION_TRIPLET(-P,password,file) Password file for workers to authenticate to master.
OPTION_TRIPLET(-t,timeout,time)Abort after this amount of idle time.
OPTION_TRIPLET(-C,config-file,file)Use the configuration file <file>.
OPTION_TRIPLET(-E,extra-options,options)Extra options that should be added to the worker.
OPTION_TRIPLET(-S,scratch,file)Scratch directory. (default is /tmp/${USER}-workers)
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
OPTION_ITEM(--worker-debug-files)Retrieve debug log file from workers. (currently only with -T condor.)
OPTION_ITEM(`-h, --help')Show this screen.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose you have a Work Queue master with a project name of "barney".
To maintain workers for barney, do this:

LONGCODE_BEGIN
work_queue_pool -T condor -M barney
LONGCODE_END

To maintain a maximum of 100 workers on an SGE batch system, do this:

LONGCODE_BEGIN
work_queue_pool -T sge -M barney -W 100
LONGCODE_END

To start workers according to the master's capacity, such that the
workers exit after 5 minutes (300s) of idleness:

LONGCODE_BEGIN
work_queue_pool -T condor -M barney -c -t 300
LONGCODE_END

If you want to start workers that match any project that begins
with barney, use a regular expression:

LONGCODE_BEGIN
work_queue_pool -T condor -M barney.\* -c -t 300
LONGCODE_END

Use the configuration file BOLD(my_conf):

LONGCODE_BEGIN
work_queue_pool -Cmy_conf
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
task-per-worker
timeout
worker-extra-options
cores
memory
disk
LONGCODE_END

SECTION(KNOWN BUGS)

The capacity measurement currently assumes single-core tasks running on single-core
workers, and behaves unexpectedly with multi-core tasks or multi-core workers.

When generating a worker pool for a foreman, specify a minimum number of workers
to run at all times.  Otherwise, the master will not assign any tasks to the foreman,
because it (initally) has no workers.

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER
