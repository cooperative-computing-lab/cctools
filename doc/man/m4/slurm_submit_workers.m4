include(manual.h)dnl
HEADER(slurm_submit_workers)

SECTION(NAME)
BOLD(slurm_submit_workers) - submit work_queue_worker to a SLURM cluster.

SECTION(SYNOPSIS)
CODE(BOLD(slurm_submit_workers [options] PARAM(servername) PARAM(port) PARAM(num-workers)))

SECTION(DESCRIPTION)
CODE(slurm_submit_workers) schedules the execution of MANPAGE(work_queue_worker,1)
on the SLURM batch system through its job submission interface, qsub.
The number of BOLD(work_queue_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. These two arguments become optional when the
auto mode option is specified for work_queue_worker.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-M, name)Name of the preferred manager for worker.
OPTION_PAIR(-c, cores)Set the number of cores each worker should use (0=auto). (default=1)
OPTION_PAIR(-C, catalog)Set catalog server for work_queue_worker to <catalog>. <catalog> format: HOSTNAME:PORT.
OPTION_PAIR(-t, seconds)Abort work_queue_worker after this amount of idle time (default=900s).
OPTION_PAIR(-d, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_PAIR(-w, size)Set TCP window size
OPTION_PAIR(-i, time)Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
OPTION_PAIR(-b, time)Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
OPTION_PAIR(-z, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_PAIR(-A, arch)Set architecture string for the worker to report to manager instead of the value in uname.
OPTION_PAIR(-O, os)Set operating system string for the worker to report to manager instead of the value in uname.
OPTION_PAIR(-s, path)Set the location for creating the working directory of the worker.
OPTION_PAIR(-p, parameters)SLURM qsub parameters.
OPTION_ITEM(-h)Show help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Submit 10 worker instances to run on SLURM and connect to a specific manager:

LONGCODE_BEGIN
slurm_submit_workers manager.somewhere.edu 9123 10
LONGCODE_END

Submit 10 work_queue_worker instances to run on SLURM in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

LONGCODE_BEGIN
slurm_submit_workers -a -t 3600 -M Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER
