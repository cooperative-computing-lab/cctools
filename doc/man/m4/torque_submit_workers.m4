include(manual.h)dnl
HEADER(torque_submit_workers)

SECTION(NAME)
BOLD(torque_submit_workers) - submit work_queue_worker to a Torque cluster.

SECTION(SYNOPSIS)
CODE(torque_submit_workers [options] PARAM(servername) PARAM(port) PARAM(num-workers))

SECTION(DESCRIPTION)
CODE(torque_submit_workers) schedules the execution of MANPAGE(work_queue_worker,1)
on the Torque batch system through its job submission interface, qsub.
The number of BOLD(work_queue_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. These two arguments become optional when the
auto mode option is specified for work_queue_worker.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_FLAG_SHORT(M) name)Name of the preferred manager for worker.
OPTION_ARG_SHORT(c, cores)Set the number of cores each worker should use (0=auto). (default=1)
OPTION_ARG_SHORT(C, catalog)Set catalog server for work_queue_worker to PARAM(catalog). PARAM(catalog) format: HOSTNAME:PORT.
OPTION_ARG_SHORT(C, catalog)Set catalog server for work_queue_worker to PARAM(catalog). PARAM(catalog) format: HOSTNAME:PORT.
OPTION_ARG_SHORT(t, seconds)Abort work_queue_worker after this amount of idle time (default=900s).
OPTION_ARG_SHORT(d, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_ARG_SHORT(w, size)Set TCP window size
OPTION_ARG_SHORT(i, time)Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
OPTION_ARG_SHORT(b, time)Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
OPTION_ARG_SHORT(z, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_ARG_SHORT(A, arch)Set architecture string for the worker to report to manager instead of the value in uname.
OPTION_ARG_SHORT(O, os)Set operating system string for the worker to report to manager instead of the value in uname.
OPTION_ARG_SHORT(s, path)Set the location for creating the working directory of the worker.
OPTION_ARG_SHORT(j)Use job array to submit workers.
OPTION_ARG_SHORT(p, parameters)Torque qsub parameters.
OPTION_ARG_LONG(scratch-dir,path)Set the scratch directory location created on the local machine. (default=${USER}-workers)
OPTION_FLAG_SHORT(h)Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Submit 10 worker instances to run on Torque and connect to a specific manager:

LONGCODE_BEGIN
torque_submit_workers manager.somewhere.edu 9123 10
LONGCODE_END

Submit 10 work_queue_worker instances to run on Torque in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

LONGCODE_BEGIN
torque_submit_workers -a -t 3600 -M Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER
