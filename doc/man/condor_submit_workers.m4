include(manual.h)dnl
HEADER(condor_submit_workers)dnl

SECTION(NAME)
BOLD(condor_submit_workers) - submit work_queue_worker to the Condor grid.

SECTION(SYNOPSIS)
CODE(BOLD(condor_submit_workers [options] PARAM(servername) PARAM(port) PARAM(num-workers)))

SECTION(DESCRIPTION)
CODE(condor_submit_workers) schedules the execution of MANPAGE(work_queue_worker,1) 
on a grid managed by Condor through its job submission interface, condor_submit.
The number of BOLD(work_queue_worker) scheduled and run is given by the BOLD(num-workers)
argument.

The BOLD(servername) and BOLD(port) arguments specify the hostname and port number of the 
master for the work_queue_worker to connect. These two arguments become optional when the 
auto mode option is specified for work_queue_worker. 

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-a)Enable auto mode for work_queue_worker.
OPTION_ITEM(-s)Run as a shared worker.
OPTION_PAIR(-N, name)Preferred project name for work_queue_worker to connect.
OPTION_PAIR(-C, catalog)Set catalog server for work_queue_worker to <catalog>. <catalog> format: HOSTNAME:PORT.
OPTION_PAIR(-t, seconds)Abort work_queue_worker after this amount of idle time (default=900s).
OPTION_PAIR(-r, requirements)Condor requirements ClassAd.
OPTION_ITEM(-h)Show help message.
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
condor_submit_workers -a -t 3600 -N Project_A 10
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_WORK_QUEUE

FOOTER
