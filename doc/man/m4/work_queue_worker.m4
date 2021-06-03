include(manual.h)dnl
HEADER(work_queue_worker)

SECTION(NAME)
BOLD(work_queue_worker) - worker process for executing tasks
dispatched through Work Queue

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_worker [options] PARAM(managerhost) PARAM(port)))

CODE(BOLD(work_queue_worker [options] PARAM(managerhost:port])))

CODE(BOLD(work_queue_worker [options] "PARAM(managerhost:port;[managerhost:port;managerhost:port;...])))"

CODE(BOLD(work_queue_worker [options] -M PARAM(projectname)))

SECTION(DESCRIPTION)

BOLD(work_queue_worker) is the worker process for executing tasks dispatched
from a manager application built using the BOLD(Work Queue) API. BOLD(work_queue_worker)
connects to the manager application, accepts, runs, and returns tasks dispatched to it.

PARA

The BOLD(managerhost) and BOLD(port) arguments specify the hostname and port
number of the manager application for work_queue_worker to connect. Several
managerhosts and ports may be specified, separated with a semicolon (;), with the
worker connecting to any of the managers specified (When specifying multiple
managers, remember to escape the ; from shell interpretation, for example, using
quotes.)

Alternatevely, the manager may be specified by name, using the BOLD(-M) option.

PARA

BOLD(work_queue_worker) can be run locally or deployed remotely on any of the
grid or cloud computing environments such as SGE, PBS, SLURM, and HTCondor using
MANPAGE(sge_submit_workers,1), MANPAGE(pbs_submit_workers,1), MANPAGE(slurm_submit_workers), and MANPAGE(condor_submit_workers,1) respectively.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-v, --version')Show version string.
OPTION_ITEM(-h, --help')Show this help message.
OPTION_TRIPLET(-N,-M, manager-name, name)Set the name of the project this worker should work for.  A worker can have multiple projects.
OPTION_TRIPLET(-C, catalog, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_TRIPLET(-d, debug, flag)Enable debugging for the given subsystem. Try -d all as a start.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_PAIR(--debug-max-rotate, bytes)Set the maximum file size of the debug log.  If the log exceeds this size, it is renamed to "filename.old" and a new logfile is opened.  (default=10M. 0 disables)
OPTION_ITEM(--debug-release-reset)Debug file will be closed, renamed, and a new one opened after being released from a manager.
OPTION_ITEM(`--foreman')Enable foreman mode.
OPTION_TRIPLET(-f, foreman-name, name)Set the project name of this foreman to PARAM(project). Implies --foreman.
OPTION_PAIR(--foreman-port, port[:highport]) Set the port for the foreman to listen on.  If PARAM(highport) is specified the port is chosen from between PARAM(port) and PARAM(highport). Implies --foreman.
OPTION_TRIPLET(-Z, foreman-port-file, file)Select port to listen to at random and write to this file.  Implies --foreman.
OPTION_TRIPLET(-F, fast-abort, mult)Set the fast abort multiplier for foreman (default=disabled).
OPTION_PAIR(--specify-log, logfile)Send statistics about foreman to this file.
OPTION_TRIPLET(-P, password, pwfile)Password file for authenticating to the manager.
OPTION_TRIPLET(-t, timeout, time)Abort after this amount of idle time. (default=900s)
OPTION_ITEM(--parent-death)Exit if parent process dies.
OPTION_TRIPLET(-w, tcp-window-size, size)Set TCP window size.
OPTION_TRIPLET(-i, min-backoff, time)Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
OPTION_TRIPLET(-b, max-backoff, time)Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
OPTION_TRIPLET(-A, arch, arch)Set the architecture string the worker reports to its supervisor. (default=the value reported by uname)
OPTION_TRIPLET(-O, os, os)Set the operating system string the worker reports to its supervisor. (default=the value reported by uname)
OPTION_TRIPLET(-s, workdir, path)Set the location where the worker should create its working directory. (default=/tmp)
OPTION_PAIR(--bandwidth, mbps)Set the maximum bandwidth the foreman will consume in Mbps. (default=unlimited)
OPTION_PAIR(--cores, n)Set the number of cores this worker should use.  Set it to 0 to have the worker use all of the available resources. (default=1)
OPTION_PAIR(--gpus, n)Set the number of GPUs this worker should use. (default=0)
OPTION_PAIR(--memory, mb)Manually set the amount of memory (in MB) reported by this worker.
OPTION_PAIR(--disk, mb)Manually set the amount of disk space (in MB) reported by this worker.
OPTION_PAIR(--wall-time, s)Set the maximum number of seconds the worker may be active.
OPTION_PAIR(--feature, feature)Specifies a user-defined feature the worker provides (option can be repeated).
OPTION_PAIR(--volatility, chance)Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).
OPTIONS_END

SECTION(FOREMAN MODE)

BOLD(work_queue_worker) can also be run in BOLD(foreman) mode, in which it connects to a
manager as a worker while acting as a manager itself.  Any tasks the foreman receives from
its manager are sent to its subordinate worker processes.

PARA

BOLD(Foreman) mode is enabled by either specifying a port to listen on using the BOLD(-f PARAM(port)) option or by
setting the mode directly with the BOLD(--foreman) option.  The foreman can be directed to advertise its
presence on the MANPAGE(catalog_server) with the BOLD(-N PARAM(project name)) flag, which other workers can use to
contact the foreman.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To run BOLD(work_queue_worker) to join a specific manager process running on host CODE(manager.somewhere.edu) port 9123:
LONGCODE_BEGIN
% work_queue_worker manager.somewhere.edu 9123
LONGCODE_END

To run BOLD(work_queue_worker) in auto mode with debugging turned on for all subsystems and
to accept tasks only from a manager application with project name set to project_A:
LONGCODE_BEGIN
% work_queue_worker -a -d all -M project_A
LONGCODE_END

To run BOLD(work_queue_worker) as a foreman working for project_A and advertising itself as foreman_A1 while listening on port 9123:
LONGCODE_BEGIN
% work_queue_worker --foreman -M project_A -N foreman_A1 -f 9123
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER
