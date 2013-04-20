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
OPTION_ITEM(-s)Run as a shared worker. By default the worker would only work on preferred projects.
OPTION_PAIR(-d, subsystem)Enable debugging for the given subsystem. Try -d all as a start.
OPTION_PAIR(-o, file)Send debugging to this file.
OPTION_PAIR(-N, project)Set the project name to PARAM(project).
OPTION_PAIR(-P, pwfile)Password file for authenticating to the master.
OPTION_PAIR(-t, time)Abort after this amount of idle time. (default=900s)
OPTION_PAIR(-w, size)Set TCP window size.
OPTION_PAIR(-i, time)Set initial value for backoff interval when worker fails to connect to a master. (default=1s)
OPTION_PAIR(-b, time)Set maxmimum value for backoff interval when worker fails to connect to a master. (default=60s)
OPTION_PAIR(-z, size)Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
OPTION_ITEM(-v)Show version string.
OPTION_ITEM(-h)Show this help message.
OPTIONS_END

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
% work_queue_worker -a -d all -N project_A 
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER

