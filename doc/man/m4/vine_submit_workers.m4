include(manual.h)dnl
HEADER(vine_submit_workers)

SECTION(NAME)
BOLD(vine_submit_workers) - submit vine_worker to the Condor, Slurm, or UGE batch systems.

SECTION(SYNOPSIS)
CODE(vine_submit_workers [batch options] [worker options] [batch specific options] PARAM(servername) PARAM(port) PARAM(num-workers))

or

CODE(vine_submit_workers [batch options] [worker options] --manager-name PARAM(name) [batch specific options] PARAM(num-workers))


SECTION(DESCRIPTION)
CODE(vine_submit_workers) schedules the execution of MANPAGE(vine_worker,1)
on Condor, Slurm, or UGE through their respective job submission interfaces.
The number of BOLD(vine_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the
manager for the vine_worker to connect. Alternatively, this information can be obtained from
the catalog server by specifying the name of the TaskVine manager using the --manager-name parameter.

SECTION(BATCH OPTIONS)
OPTIONS_BEGIN
OPTION_ARG(T, batch-type, batch)Name of the batch system to submit workers. Out of (condor, slurm, uge).
OPTIONS_END

SECTION(WORKER OPTIONS)
OPTIONS_BEGIN
OPTION_ARG(M, manager-name, name)Name of the preferred manager for worker.
OPTION_ARG(N, name, name)Same as -M (backwards compatibility).
OPTION_ARG(C, catalog, catalog)Set catalog server to PARAM(catalog). PARAM(catalog) format: HOSTNAME:PORT.
OPTION_ARG(t, timeout, time)Abort after this amount of idle time (default=900s).
OPTION_ARG(d, debug, subsystem)Enable debugging on worker for this subsystem (try -d all to start).
OPTION_ARG(w, tcp-window-size, size)Set TCP window size
OPTION_ARG(i, min-backoff, time)Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
OPTION_ARG(b, max-backoff, time)Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
OPTION_ARG(z, disk-threshold, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_ARG(A, arch, arch)Set architecture string for the worker to report to manager instead of the value in uname.
OPTION_ARG(O, os, os)Set operating system string for the worker to report to manager instead of the value in uname.
OPTION_ARG(s, workdir, path)Set the location for creating the working directory of the worker.
OPTION_ARG(P, password, pwfile)Password file to authenticate workers to manager.
OPTION_ARG(ssl) Use ssl to communicate with manager.
OPTION_ARG_LONG(cores, cores)Set the number of cores each worker should use (0=auto). (default=1)
OPTION_ARG_LONG(memory, size)Manually set the amonut of memory (in MB) reported by this worker.
OPTION_ARG_LONG(disk, size)Manually set the amount of disk (in MB) reported by this worker.
OPTION_ARG_LONG(scratch-dir,path)Set the scratch directory location created on the local machine. (default=/tmp/${USER}-workers)
OPTION_ARG(E, worker-options, str)Extra options passed to vine_worker
OPTION_FLAG(h,help)Show help message.
OPTIONS_END

SECTION(BATCH SPECIFIC OPTIONS)
SECTION(CONDOR)
OPTION_ARG(r,requirements,reqs)Condor requirements expression.
OPTION_ARG_LONG(class-ad,ad)Extra condor class ad. May be specified multiple times.
OPTION_FLAG_LONG(autosize)Condor will automatically size the worker to the slot.
OPTION_ARG_LONG(docker-universe,image)Run worker inside PARAM(image) using condor's docker universe

SECTION(SLURM)
OPTION_ARG(j)Use job array to submit workers.
OPTION_ARG(p, parameters)SLURM sbatch parameters.

SECTION(UGE)
OPTION_ARG(j)Use job array to submit workers.
OPTION_ARG(p, parameters)UGE qsub parameters.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero. On failure, returns non-zero.

SECTION(EXAMPLES)

Submit 10 worker instances to run on Condor and connect to a specific manager:

LONGCODE_BEGIN
vine_submit_workers -T condor manager.somewhere.edu 9123 10
LONGCODE_END

Submit 10 vine_worker instances to run on Condor in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

LONGCODE_BEGIN
vine_submit_workers -T condor -a -t 3600 -M Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_TASK_VINE

FOOTER
