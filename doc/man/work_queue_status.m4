include(manual.h)dnl
HEADER(work_queue_status)

SECTION(NAME) 
BOLD(work_queue_status) - display status of currently running Work Queue applications.

SECTION(SYNOPSIS)
CODE(BOLD(work_queue_status [options] [master] [port]))

SECTION(DESCRIPTION)

BOLD(work_queue_status) displays the status of currently running Work Queue applications.
When run with no options, it queries the global catalog server to list the currently running
Work Queue masters.  When given an address and a port, it queries a master directly to obtain
more detailed information about tasks and workers.

LIST_BEGIN
LIST_ITEM()Hostname and port number of the application.
LIST_ITEM()Number of waiting tasks.
LIST_ITEM()Number of completed tasks.
LIST_ITEM()Number of connected workers.
LIST_ITEM()Number of busy workers currently executing a task.
LIST_ITEM()Timestamp of when there was last communication with the application.
LIST_END

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_PAIR(-C, catalog)Set catalog server to <catalog>. Format: HOSTNAME:PORT
OPTION_ITEM(-Q)Show summary information about queues. (default)
OPTION_ITEM(-T)Show details of all tasks in the queue.
OPTION_ITEM(-W)Show details of all workers connected to the master.
OPTION_PAIR(-d, subsystem)Enable debugging for the given subsystem. Try -d all as a start.
OPTION_PAIR(-t, time)RPC timeout (default=300s).
OPTION_ITEM(-l)Long output.
OPTION_ITEM(-h)Show this help message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WORK_QUEUE

FOOTER

