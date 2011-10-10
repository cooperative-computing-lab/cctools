include(manual.h)dnl
HEADER(work_queue_pool)dnl

SECTION(NAME)
BOLD(work_queue_pool) - submit a pool of Work Queue workers on various batch systems.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_pool [options] PARAM(hostname) PARAM(port) PARAM(number)))
PARA
or
PARA
CODE(BOLD(work_queue_pool [options] -a PARAM(num-workers)))

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
out the appropriate masters (see the BOLD(CODE(-N)) option). In either case,
the BOLD(PARAM(number)) argument specifies the number of workers that
BOLD(work_queue_pool) should maintain.

SECTION(OPTIONS)

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_PAIR(-d, subsystem)Enable debugging for this subsystem.
OPTION_PAIR(-S, scratch)Scratch directory. (default is /tmp/${USER}-workers)
OPTION_PAIR(-T, type)Batch system type: unix, condor, sge, workqueue, xgrid. (default is unix)
OPTION_PAIR(-r, count)Number of attemps to retry if failed to submit a worker.
OPTION_PAIR(-W, path)Path to the MANPAGE(work_queue_worker,1) executable.
OPTION_ITEM(-h)Show this screen.
OPTIONS_END

SUBSECTION(Worker Options)
OPTIONS_BEGIN
OPTION_ITEM(-a)Enable auto mode. In this mode the workers would ask a catalog server for available masters.
OPTION_PAIR(-t, time)Abort after this amount of idle time.
OPTION_PAIR(-C, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT 
OPTION_PAIR(-N, project)Name of a preferred project. A worker can have multiple preferred projects.
OPTION_ITEM(-s)Run as a shared worker. By default the workers would only work for preferred projects.
OPTION_PAIR(-o, file)Send debugging to this file.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)
SUBSECTION(Example 1)
Suppose you have a Work Queue master running on CODE(barney.nd.edu) and it is
listening on port CODE(9123). To start 10 workers on the Condor batch system
for your master, you can invoke BOLD(work_queue_pool) like this: 

LONGCODE_BEGIN
work_queue_pool -T Condor barney.nd.edu 9123 10
LONGCODE_END

If you want to start the 10 workers on the SGE batch system instead, you only
need to change the BOLD(CODE(-T)) option:

LONGCODE_BEGIN
work_queue_pool -T SGE barney.nd.edu 9123 10
LONGCODE_END

If you have access to both of the Condor and SGE systems, you can run both of
the above commands and you will then get 20 workers for your master.

SUBSECTION(Example 2)
Suppose you have started a Work Queue master with MANPAGE(makeflow,1) like this:

LONGCODE_BEGIN
makeflow -T wq -a -N myproject makeflow.script
LONGCODE_END

The BOLD(CODE(-N)) option given to BOLD(makeflow) specifies the project name for the
Work Queue master. The master's information, such as CODE(hostname) and
CODE(port), will be reported to a catalog server. The BOLD(work_queue_pool)
program can start workers that prefer to work for this master by specifying the
same project name on the command line (see the BOLD(CODE(-N)) option):

LONGCODE_BEGIN
work_queue_pool -T Condor -N my_project -a 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE
