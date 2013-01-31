include(manual.h)dnl
HEADER(makeflow)

SECTION(NAME)
BOLD(makeflow) - workflow engine for executing distributed workflows

SECTION(SYNOPSIS)
CODE(BOLD(makeflow [options] PARAM(dagfile)))

SECTION(DESCRIPTION)

BOLD(Makeflow) is a workflow engine for distributed computing. It accepts a
specification of a large amount of work to be performed, and runs it on remote
machines in parallel where possible. In addition, BOLD(Makeflow) is
fault-tolerant, so you can use it to coordinate very large tasks that may run
for days or weeks in the face of failures. BOLD(Makeflow) is designed to be
similar to Make, so if you can write a Makefile, then you can write a
BOLD(Makeflow).

PARA

You can run a BOLD(Makeflow) on your local machine to test it out. If you have
a multi-core machine, then you can run multiple tasks simultaneously. If you
have a Condor pool or a Sun Grid Engine batch system, then you can send your
jobs there to run. If you don't already have a batch system, BOLD(Makeflow)
comes with a system called Work Queue that will let you distribute the load
across any collection of machines, large or small.

SECTION(OPTIONS)
When CODE(makeflow) is ran without arguments, it will attempt to execute the
workflow specified by the BOLD(Makeflow) dagfile using the CODE(local)
execution engine.

SUBSECTION(Commands)
OPTIONS_BEGIN
OPTION_ITEM(-c)Clean up: remove logfile and all targets.
OPTION_ITEM(-D)Display the Makefile as a Dot graph.
OPTION_ITEM(-f)Write summary of workflow to file.
OPTION_ITEM(-k)Syntax check.
OPTION_ITEM(-h)Show this help screen.
OPTION_ITEM(-I)Show input files.
OPTION_ITEM(-m)Email summary of workflow to address.
OPTION_ITEM(-O)Show output files.
OPTION_ITEM(-v)Show version string.
OPTIONS_END

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_PAIR(-B, options)Add these options to all batch submit files.
OPTION_PAIR(-j, #)Max number of local jobs to run at once. (default is # of cores)
OPTION_PAIR(-J, #)Max number of remote jobs to run at once. (default is 100)
OPTION_PAIR(-l, logfile)Use this file for the makeflow log. (default is X.makeflowlog)
OPTION_PAIR(-L, logfile)Use this file for the batch system log. (default is X.PARAM(type)log)
OPTION_ITEM(-R)Automatically retry failed batch jobs up to 100 times.
OPTION_PAIR(-r, n)Automatically retry failed batch jobs up to n times.
OPTION_PAIR(-S, timeout)Time to retry failed batch job submission. (default is 3600s)
OPTION_PAIR(-T, type)Batch system type: local, condor, sge, moab, cluster, wq, hadoop, mpi-queue. (default is local)
OPTIONS_END

SUBSECTION(Debugging Options)
OPTIONS_BEGIN
OPTION_PAIR(-d, subsystem)Enable debugging for this subsystem.
OPTION_PAIR(-o, file)Send debugging to this file.
OPTIONS_END

SUBSECTION(WorkQueue Options)
OPTIONS_BEGIN
OPTION_ITEM(-a)Advertise the master information to a catalog server.
OPTION_PAIR(-C, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_PAIR(-F, #)WorkQueue fast abort multiplier. (default is deactivated)
OPTION_PAIR(-N, project)Set the project name to PARAM(project).
OPTION_PAIR(-p, port)Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
OPTION_PAIR(-Z,file)Select port at random and write it to this file.  (default is disabled)
OPTION_PAIR(-P, integer)Priority. Higher the value, higher the priority.
OPTION_PAIR(-w, mode)Auto WorkQueue mode. Mode is either 'width' or 'group' (DAG [width] or largest [group] of tasks).
OPTION_PAIR(-W, mode)WorkQueue scheduling algorithm. (time|files|fcfs)
OPTIONS_END

SUBSECTION(Other Options)
OPTIONS_BEGIN
OPTION_ITEM(-A)Disable the check for AFS. (experts only)
OPTION_ITEM(-K)Preserve (i.e., do not clean) intermediate symbolic links.
OPTION_ITEM(-z)Force failure on zero-length output files.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

The following environment variables will affect the execution of your
BOLD(Makeflow):
SUBSECTION(BATCH_OPTIONS)

This corresponds to the BOLD(-B) PARAM(options) parameter and will pass extra
batch options to the underlying execution engine.

SUBSECTION(MAKEFLOW_MAX_LOCAL_JOBS)
This corresponds to the BOLD(-j) PARAM(#) parameter and will set the maximum
number of local batch jobs.  If a BOLD(-j) PARAM(#) parameter is specified, the
minimum of the argument and the environment variable is used.

SUBSECTION(MAKEFLOW_MAX_REMOTE_JOBS)
This corresponds to the BOLD(-J) PARAM(#) parameter and will set the maximum
number of local batch jobs.  If a BOLD(-J) PARAM(#) parameter is specified, the
minimum of the argument and the environment variable is used.
PARA
Note that variables defined in your BOLD(Makeflow) are exported to the
environment.

SUBSECTION(TCP_LOW_PORT)
Inclusive low port in range used with CODE(-p 0).

SUBSECTION(TCP_HIGH_PORT))
Inclusive high port in range used with CODE(-p 0).

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Run makeflow locally with debugging:
LONGCODE_BEGIN
makeflow -d all Makeflow
LONGCODE_END
   
Run makeflow on Condor will special requirements:
LONGCODE_BEGIN
makeflow -T condor -B "requirements = MachineGroup == 'ccl'" Makeflow
LONGCODE_END

Run makeflow with WorkQueue using named workers:
LONGCODE_BEGIN
makeflow -T wq -a -N project.name Makeflow
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER

