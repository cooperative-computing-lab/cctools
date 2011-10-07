include(manual.h)dnl
HEADER(work_queue_worker)

SECTION(NAME) 
BOLD(work_queue_worker) - worker framework for executing tasks
dispatched through Work Queue

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_worker [options] PARAM(masterhost) PARAM(port)))

SECTION(DESCRIPTION)

BOLD(work_queue_worker) is the worker framework for executing tasks dispatched
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
OPTION_PAIR(-N, project)Set the project name to PARAM(project).
OPTION_ITEM(-s)Run as a shared worker. By default the worker would only work on preferred projects.
OPTION_PAIR(-t, time)Abort after this amount of idle time. (default=900s)
OPTION_PAIR(-o, file)Send debugging to this file.
OPTION_PAIR(-w, size)Set TCP window size.
OPTION_PAIR(-z, size)Set available disk space threshold (in MB) before aborting. By default no checks on available space are done.
OPTION_ITEM(-v)Show version string.
OPTION_ITEM(-h)Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
To run BOLD(work_queue_worker) in auto mode with debugging turned on for all subsystems and
to accept tasks only from a master application with project name set to project_A and connect 
to this master on xyz.abc.edu on port 9500:
LONGCODE_BEGIN
./work_queue_worker -d all -N project_A xyz.abc.edu 9500
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM()LINK(Work Queue User Manual,"http://www.cse.nd.edu/~ccl/software/manuals/workqueue.html")
LIST_END
