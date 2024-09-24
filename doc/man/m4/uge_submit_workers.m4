include(manual.h)dnl
HEADER(uge_submit_workers)

SECTION(NAME)
BOLD(uge_submit_workers) - submit work_queue_worker to a Univa Grid Engine (UGE) cluster.

SECTION(SYNOPSIS)
CODE(uge_submit_workers [options] PARAM(servername) PARAM(port) PARAM(num-workers))

when auto mode is not enabled for the worker, or

CODE(uge_submit_workers [options] PARAM(num-workers))

when auto mode is enabled for the worker.

SECTION(DESCRIPTION)
CODE(uge_submit_workers) schedules the execution of MANPAGE(work_queue_worker,1)
on the Univa Grid Engine (UGE) through its job submission interface, qsub.
The number of BOLD(work_queue_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. These two arguments become optional when the
auto mode option is specified for work_queue_worker.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG(M, manager-name, name)Name of the preferred manager for worker. (auto mode enabled)
OPTION_ARG(C, catalog, catalog)Set catalog server for work_queue_worker to PARAM(catalog). PARAM(catalog) format: HOSTNAME:PORT.
OPTION_ARG(C, catalog, catalog)Set catalog server for work_queue_worker to PARAM(catalog). PARAM(catalog) format: HOSTNAME:PORT.
OPTION_ARG(t, timeout, seconds)Abort work_queue_worker after this amount of idle time (default=900s).
OPTION_ARG(d, debug, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_ARG(w, tcp-window-size, size)Set TCP window size
OPTION_ARG(i, min-backoff, time)Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
OPTION_ARG(b, max-backoff, time)Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
OPTION_ARG(z, disk-threshold, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_ARG(A, arch, arch)Set architecture string for the worker to report to manager instead of the value in uname.
OPTION_ARG(O, os, os)Set operating system string for the worker to report to manager instead of the value in uname.
OPTION_ARG(s, workdir, path)Set the location for creating the working directory of the worker.
OPTION_ARG(P,--password, file)Password file to authenticate workers to manager.
OPTION_ARG_LONG(cores, cores)Set the number of cores each worker should use (0=auto). (default=1)
OPTION_ARG_LONG(memory, size)Manually set the amonut of memory (in MB) reported by this worker.
OPTION_ARG_LONG(disk, size)Manually set the amount of disk (in MB) reported by this worker.
OPTION_ARG_LONG(scratch-dir, path)Set the scratch directory location created on the local machine. (default=${USER}-workers) 
OPTION_FLAG_SHORT(j)Use job array to submit workers.
OPTION_ARG_SHORT(-p, parameters)UGE qsub parameters.
OPTION_FLAG(h,help)Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Submit 10 worker instances to run on UGE and connect to a specific manager:

LONGCODE_BEGIN
uge_submit_workers manager.somewhere.edu 9123 10
LONGCODE_END

Submit 10 work_queue_worker instances to run on UGE in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

LONGCODE_BEGIN
uge_submit_workers -a -t 3600 -M Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER
