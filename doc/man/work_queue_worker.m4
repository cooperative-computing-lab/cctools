include(manual.h)dnl
HEADER(work_queue_worker)

SECTION(NAME) 
BOLD(work_queue_worker) - worker process for executing tasks
dispatched through Work Queue

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_worker [options] PARAM(masterhost) PARAM(port)))

SECTION(DESCRIPTION)

BOLD(work_queue_worker) is the worker process for executing tasks dispatched
from a master application built using the BOLD(Work Queue) API. BOLD(work_queue_worker) 
connects to the master application, accepts, runs, and returns tasks dispatched to it. 

PARA

The BOLD(masterhost) and BOLD(port) arguments specify the hostname and port number 
of the master application for work_queue_worker to connect. These two arguments 
become optional when the auto mode option is specified.

PARA

BOLD(work_queue_worker) can be run locally or deployed remotely on any of the
grid or cloud computing environments such as SGE, Amazon EC2, Condor using
MANPAGE(sge_submit_workers,1), MANPAGE(ec2_submit_workers,1), 
MANPAGE(condor_submit_workers,1) respectively.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-a)Enable auto mode. In this mode the worker would ask a catalog server for available masters.
OPTION_PAIR(-C, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_PAIR(-d, subsystem)Enable debugging for the given subsystem. Try -d all as a start.
OPTION_PAIR(-o, file)Send debugging to this file.
OPTION_PAIR(--debug-file-size, bytes)Set the maximum file size of the debug log.  If the log exceeds this size, it is renamed to "filename.old" and a new logfile is opened.  (default=10M. 0 disables)
OPTION_PAIR(-M, project)Set the name of the project this worker should work for.  A worker can have multiple projects.
OPTION_PAIR(-N, project)Set the project name of this foreman to PARAM(project).  If in worker, classic or auto mode, behaves as BOLD(-M) for backwards compatibility.
OPTION_PAIR(-P, pwfile)Password file for authenticating to the master.
OPTION_ITEM(--foreman)Enable foreman mode.
OPTION_PAIR(-f, port[:highport]) Set the port for the foreman to listen on.  If PARAM(highport) is specified the port is chosen from between PARAM(port) and PARAM(highport).
OPTION_PAIR(-t, time)Abort after this amount of idle time. (default=900s)
OPTION_PAIR(-w, size)Set TCP window size.
OPTION_PAIR(-i, time)Set initial value for backoff interval when worker fails to connect to a master. (default=1s)
OPTION_PAIR(-b, time)Set maxmimum value for backoff interval when worker fails to connect to a master. (default=60s)
OPTION_PAIR(-z, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_PAIR(-A, arch)Set the architecture string the worker reports to its supervisor. (default=the value reported by uname)
OPTION_PAIR(-O, os)Set the operating system string the worker reports to its supervisor. (default=the value reported by uname)
OPTION_PAIR(-s, path)Set the location where the worker should create its working directory. (default=/tmp)
OPTION_ITEM(-v)Show version string.
OPTION_ITEM(-h)Show this help message.
OPTION_PAIR(--bandwidth, mbps)Set the maximum bandwidth the foreman will consume in Mbps. (default=unlimited)
OPTION_PAIR(--cores, n)Set the number of cores this worker should use.  Set it to 0 to have the worker use all of the available resources. (default=1)
OPTION_PAIR(--memory, mb)Manually set the amount of memory (in MB) reported by this worker.
OPTION_PAIR(--disk, mb)Manually set the amount of disk space (in MB) reported by this worker.
OPTIONS_END

SECTION(FOREMAN MODE)

BOLD(work_queue_worker) can also be run in BOLD(foreman) mode, in which it connects to a
master as a worker while acting as a master itself.  Any tasks the foreman receives from
its master are sent to its subordinate worker processes.

PARA

BOLD(Foreman) mode is enabled by either specifying a port to listen on using the BOLD(-f PARAM(port)) option or by
setting the mode directly with the BOLD(--foreman) option.  The foreman can be directed to advertise its
presence on the MANPAGE(catalog_server) with the BOLD(-N PARAM(project name)) flag, which other workers can use to
contact the foreman.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To run BOLD(work_queue_worker) to join a specific master process running on host CODE(master.somewhere.edu) port 9123:
LONGCODE_BEGIN
% work_queue_worker master.somewhere.edu 9123
LONGCODE_END

To run BOLD(work_queue_worker) in auto mode with debugging turned on for all subsystems and
to accept tasks only from a master application with project name set to project_A:
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

