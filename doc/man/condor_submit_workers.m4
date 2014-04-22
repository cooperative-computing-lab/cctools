include(manual.h)dnl
HEADER(condor_submit_workers)dnl

SECTION(NAME)
BOLD(condor_submit_workers) - submit work_queue_worker to the Condor grid.

SECTION(SYNOPSIS)
LONGCODE_BEGIN
CODE(BOLD(condor_submit_workers [options] PARAM(servername) PARAM(port) PARAM(num-workers)))
LONGCODE_END

or

LONGCODE_BEGIN
CODE(BOLD(condor_submit_workers [options] --master-name PARAM(name) PARAM(num-workers)))
LONGCODE_END


SECTION(DESCRIPTION)
CODE(condor_submit_workers) schedules the execution of MANPAGE(work_queue_worker,1)
on a grid managed by Condor through its job submission interface, condor_submit.
The number of BOLD(work_queue_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the
master for the work_queue_worker to connect. Alternatively, this information can be obtained from
the catalog server by specifying the name of the work queue using the --master-name parameter.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_TRIPLET(-M, master-name, name)Name of the preferred master for worker.
OPTION_TRIPLET(-N, name, name)Same as -M (backwards compatibility).
OPTION_TRIPLET(-C, catalog, catalog)Set catalog server to <catalog>. <catalog> format: HOSTNAME:PORT.
OPTION_TRIPLET(-t, timeout, time)Abort after this amount of idle time (default=900s).
OPTION_TRIPLET(-d, debug, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_TRIPLET(-w, tcp-window-size, size)Set TCP window size
OPTION_TRIPLET(-i, min-backoff, time)Set initial value for backoff interval when worker fails to connect to a master. (default=1s)
OPTION_TRIPLET(-b, max-backoff, time)Set maxmimum value for backoff interval when worker fails to connect to a master. (default=60s)
OPTION_TRIPLET(-z, disk-threshold, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_TRIPLET(-A, arch, arch)Set architecture string for the worker to report to master instead of the value in uname.
OPTION_TRIPLET(-O, os, os)Set operating system string for the worker to report to master instead of the value in uname.
OPTION_TRIPLET(-s, workdir, path)Set the location for creating the working directory of the worker.
OPTION_TRIPLET(-P,--password, file)Password file to authenticate workers to master.
OPTION_PAIR(--cores, cores)Set the number of cores each worker should use (0=auto). (default=1)
OPTION_PAIR(--memory, size)Manually set the amonut of memory (in MB) reported by this worker.
OPTION_PAIR(--disk, size)Manually set the amount of disk (in MB) reported by this worker.
OPTION_ITEM(`-h,--help')Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Submit 10 worker instances to run on Condor and connect to a specific master:

LONGCODE_BEGIN
condor_submit_workers master.somewhere.edu 9123 10
LONGCODE_END

Submit 10 work_queue_worker instances to run on Condor in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

LONGCODE_BEGIN
condor_submit_workers -a -t 3600 -M Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER

