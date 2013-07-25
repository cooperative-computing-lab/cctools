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
OPTION_TRIPLET(-b, bundle-dir, directory)Create portable bundle of workflow.
OPTION_ITEM(`-c, --clean')Clean up: remove logfile and all targets.
OPTION_TRIPLET(-f, summary-log, file)Write summary of workflow to file.
OPTION_ITEM(`-h, --help')Show this help screen.
OPTION_ITEM(`-I, --show-input')Show input files.
OPTION_TRIPLET(-m, email, email)Email summary of workflow to address.
OPTION_ITEM(`-O, --show-output')Show output files.
OPTION_ITEM(`-v, --version')Show version string.
OPTIONS_END

SUBSECTION(Batch Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-B, batch-options, options)Add these options to all batch submit files.
OPTION_TRIPLET(-j, max-local, #)Max number of local jobs to run at once. (default is # of cores)
OPTION_TRIPLET(-J, max-remote, #)Max number of remote jobs to run at once. (default is 100)
OPTION_TRIPLET(-l, makeflow-log, logfile)Use this file for the makeflow log. (default is X.makeflowlog)
OPTION_TRIPLET(-L, batch-log, logfile)Use this file for the batch system log. (default is X.PARAM(type)log)
OPTION_ITEM(`-R, --retry')Automatically retry failed batch jobs up to 100 times.
OPTION_TRIPLET(-r, retry-count, n)Automatically retry failed batch jobs up to n times.
OPTION_TRIPLET(-S, submission-timeout, timeout)Time to retry failed batch job submission. (default is 3600s)
OPTION_TRIPLET(-T, batch-type, type)Batch system type: local, condor, sge, moab, cluster, wq, hadoop, mpi-queue. (default is local)
OPTIONS_END

SUBSECTION(Debugging Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-d, debug, subsystem)Enable debugging for this subsystem.
OPTION_TRIPLET(-o, debug-file, file)Send debugging to this file.
OPTIONS_END

SUBSECTION(WorkQueue Options)
OPTIONS_BEGIN
OPTION_ITEM(`-a, --advertise')Advertise the master information to a catalog server.
OPTION_TRIPLET(-C, catalog-server, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_TRIPLET(-F, wq-fast-abort, #)WorkQueue fast abort multiplier. (default is deactivated)
OPTION_TRIPLET(-N, project-name, project)Set the project name to PARAM(project).
OPTION_TRIPLET(-p, port, port)Port number to use with WorkQueue. (default is 9123, 0=arbitrary)
OPTION_TRIPLET(-Z, port-file, file)Select port at random and write it to this file.  (default is disabled)
OPTION_TRIPLET(-P, priority, integer)Priority. Higher the value, higher the priority.
OPTION_TRIPLET(-W, wq-schedule, mode)WorkQueue scheduling algorithm. (time|files|fcfs)
OPTION_TRIPLET(-s, password, pwfile)Password file for authenticating workers.
OPTIONS_END

SUBSECTION(Monitor Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-M, monitor, dir)Enable the resource monitor, and write the monitor logs to <dir>
OPTION_PAIR(--monitor-limits, file)Use <file> as value-pairs for resource limits.
OPTION_ITEM(`--monitor-with-time-series')Enable monitor time series.                 (default is disabled)
OPTION_ITEM(`--monitor-with-opened-files')Enable monitoring of openened files.        (default is disabled)
OPTION_PAIR(--monitor-interval, #)Set monitor interval to <#> seconds. (default 1 second)
OPTION_PAIR(--monitor-log-fmt, fmt)Format for monitor logs. (default resource-rule-%06.6d, %d -> rule number)
OPTIONS_END

SUBSECTION(Other Options)
OPTIONS_BEGIN
OPTION_ITEM(`-A, --disable-afs-check')Disable the check for AFS. (experts only)
OPTION_ITEM(`-k, --syntax-check')Syntax check.
OPTION_ITEM(`-K, --preserve-links')Preserve (i.e., do not clean) intermediate symbolic links.
OPTION_ITEM(`-z, --zero-length-error')Force failure on zero-length output files.
OPTIONS_END

SUBSECTION(Display Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-D, display, opt)Display the Makefile as a Dot graph or a PPM completion graph. <opt> is one of:
   dot      Standard Dot graph
   ppm      Display a completion graph in PPM format
OPTION_ITEM(`--dot-merge-similar')Condense similar boxes
OPTION_ITEM(`--dot-proportional')Change the size of the boxes proportional to file size
OPTION_ITEM(` ')The following options for ppm generation are mutually exclusive:
OPTION_PAIR(--ppm-highlight-row, row)Highlight row <row> in completion grap
OPTION_PAIR(--ppm-highlight-file,file)Highlight node that creates file <file> in completion graph
OPTION_PAIR(--ppm-highlight-executable,exe)Highlight executable <exe> in completion grap
OPTION_ITEM(`--ppm-show-levels')Display different levels of depth in completion graph
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

Create a directory containing all of the dependencies required to run the
specified makeflow
LONGCODE_BEGIN
makeflow -b bundle Makeflow
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER

