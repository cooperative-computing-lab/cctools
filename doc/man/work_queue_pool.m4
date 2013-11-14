include(manual.h)dnl
HEADER(work_queue_pool)dnl

SECTION(NAME)
BOLD(work_queue_pool) - submit a pool of Work Queue workers on various batch systems.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_pool [options] PARAM(hostname) PARAM(port) PARAM(number)))
PARA
or
PARA
CODE(BOLD(work_queue_pool [options] -M PARAM(project-name) PARAM(num-workers)))
PARA
or
PARA
CODE(BOLD(work_queue_pool [options] -A [-c PARAM(file)]))


SECTION(DESCRIPTION)
BOLD(work_queue_pool) submits and maintains a number 
of MANPAGE(work_queue_worker,1) processes on various batch systems, such as
Condor and SGE.  Each BOLD(work_queue_worker) process represents a Work Queue
worker.  All the Work Queue workers managed by a BOLD(work_queue_pool) process
can be pointed to a specific Work Queue master, or be instructed to find their
preferred masters through a catalog server. 
PARA
If the BOLD(PARAM(hostname)) and BOLD(PARAM(port)) arguments are provided, the
workers maintained by the BOLD(work_queue_pool) process would only work for the
master running at BOLD(PARAM(hostname):PARAM(port)). If the BOLD(CODE(-a))
option is present, then the BOLD(PARAM(hostname)) and BOLD(PARAM(port))
arguments are not needed and the workers would contact a catalog server to find
out the appropriate masters (see the BOLD(CODE(-M)) option). In either case,
the BOLD(PARAM(number)) argument specifies the number of workers that
BOLD(work_queue_pool) should maintain.  
PARA
If a BOLD(work_queue_worker) process managed by the BOLD(work_queue_pool) is
shutdown (i.e. failure, eviction, etc.), then the BOLD(work_queue_pool) will
re-submit a new BOLD(work_queue_worker) to the specified batch system
BOLD(PARAM(type)) in order to maintain a constant BOLD(PARAM(number)) of
BOLD(work_queue_worker) processes.

SECTION(OPTIONS)

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-d, debug, flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-l,logfile, logfile)Log work_queue_pool status to logfile. 
OPTION_TRIPLET(-S, scratch, file)Scratch directory. (default is /tmp/${USER}-workers)
OPTION_TRIPLET(-T, batch-type, type)Batch system type: unix, condor, sge, workqueue, xgrid. (default is unix)
OPTION_TRIPLET(-r, retry, count)Number of attemps to retry if failed to submit a worker.
OPTION_TRIPLET(-m, workers-per-job, count)Each batch job will start <count> local workers. (default is 1)
OPTION_TRIPLET(-W, worker-executable, path)Path to the MANPAGE(work_queue_worker,1) executable.
OPTION_ITEM(`-A, --auto-pool-feature')Run in auto pool mode, according to the configuration file given by -c (see below, work_queue_pool.conf if -c not given).
OPTION_TRIPLET(-c, config, config)Path to the auto pool configuration file. Implies -A.
OPTION_ITEM(`-q, --one-shot')Gurantee <count> running workers and quit. The workers would terminate after their idle timeouts unless the user explicitly shuts them down.
OPTION_ITEM(`-h, --help')Show this screen.
OPTIONS_END

SUBSECTION(Worker Options)
OPTIONS_BEGIN
OPTION_ITEM(`-a, --advertise')Enable auto mode. In this mode the workers would ask a catalog server for available masters. (deprecated, implied by -M,-A).
OPTION_TRIPLET(-t, timeout, time)Abort after this amount of idle time.
OPTION_TRIPLET(-C, catalog, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_TRIPLET(-M, master-name, project)Name of a preferred project. A worker can have multiple preferred projects.
OPTION_ITEM(`-N')Same as -M,--master-name (deprecated).
OPTION_TRIPLET(-o, debug-file, file)Send debugging to this file.
OPTION_ITEM(`-E, --extra-options')Extra options that should be added to the worker.
OPTIONS_END

SUBSECTION(Auto pool feature)

work_queue_pool has the ability to maintain workers to several masters/foremen
as needed, even when the multiple master/foremen report the same name to the
catalogue. This is enabled by creating a pool configuration file, and using the
-A option. By default, -A tries to read the configuration file
`work_queue_pool.conf' in the current working directory. The -c option can be
used to specify a different path. The configuration file is a list of key-value
pairs, one pair per line, and the value separated by the key with a colon (:).
The possible valid keys are:

OPTIONS_BEGIN
OPTION_ITEM(` min_workers:') The minimum number of workers to maintain. This is
the only required key, and its value has to be greater than zero.
OPTION_ITEM(` max_workers:') The maximum number of workers to maintain for the whole pool. The default is 100.
OPTION_ITEM(` distribution:') A coma separated list of
<master_name>=<num_workers>, in which <num_workers> is the maximum number of
workers assigned to the master with <master_name>. The <master_name>
specification allows some basic regular expression substitutions ('.' for any
character, '*' for zero or more of the previous character, '?' for one or more
of the previous character.
OPTION_ITEM(` default_capacity:') The initial capacity of the masters. Capacity
is the maximum number of workers that can connect to the master such that no
worker is idle. The default is 0.
OPTION_ITEM(` ignore_capacity:') Boolean yes|no value. The default is no.
OPTION_ITEM(` mode:') One of fixed|on-demand. If on-demand (the default),
work_queue_pool adjust the observed capacity of the master as tasks are
dispatched/completed, until the number of workers assigned to the master equal
that of its distribution specification (see above). If fixed, the number of
workers assigned is immediately  the one given in the distribution.
OPTION_ITEM(` max_change_per_min:') For on-demand mode, this fields indicated
the maximum number of workers that can be submitted per minute.
OPTIONS_END


SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
SUBSECTION(Example 1)
Suppose you have a Work Queue master running on CODE(barney.nd.edu) and it is
listening on port CODE(9123). To start 10 workers on the Condor batch system
for your master, you can invoke BOLD(work_queue_pool) like this: 

LONGCODE_BEGIN
work_queue_pool -T condor barney.nd.edu 9123 10
LONGCODE_END

If you want to start the 10 workers on the SGE batch system instead, you only
need to change the BOLD(CODE(-T)) option:

LONGCODE_BEGIN
work_queue_pool -T sge barney.nd.edu 9123 10
LONGCODE_END

If you have access to both of the Condor and SGE systems, you can run both of
the above commands and you will then get 20 workers for your master.

SUBSECTION(Example 2)
Suppose you have started a Work Queue master with MANPAGE(makeflow,1) like this:

LONGCODE_BEGIN
makeflow -T wq -N myproject makeflow.script
LONGCODE_END

The BOLD(CODE(-N)) option given to BOLD(makeflow) specifies the project name for the
Work Queue master. The master's information, such as CODE(hostname) and
CODE(port), will be reported to a catalog server. The BOLD(work_queue_pool)
program can start workers that prefer to work for this master by specifying the
same project name on the command line (see the BOLD(CODE(-N)) option):

LONGCODE_BEGIN
work_queue_pool -T condor -N my_project 10
LONGCODE_END

Suppose you have two masters with project names "project-a" and "project-b",
and you would like 70 workers assigned to project-a, and 30 to project-b. You
could write a 'work_queue_pool.conf' file with the following contents:

LONGCODE_BEGIN
distribution: project-a=70, project-b=30
max_workers: 100
min_workers: 2
LONGCODE_END

And simply run:

LONGCODE_BEGIN
work_queue_pool -T condor -A
LONGCODE_END

Now, suppose you have several masters (or foremen) with names such as
"project-1", "project-2", etc., and you would like to assign the same number of
workers to all of them as they are launched, with at least 50 workers running
all the time, but with no more than 400 workers running simultaneously.
Furthermore, you would like to reuse workers as some of the masters finish
their computation. Using the auto pool feature:

LONGCODE_BEGIN
distribution: project.*=400
max_workers: 400
min_workers: 50
LONGCODE_END

Note that the previous works even when not all the masters have distinct names.


SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER

