include(manual.h)dnl
HEADER(vine_status)

SECTION(NAME)
BOLD(vine_status) - display status of currently running TaskVine applications.

SECTION(SYNOPSIS)
CODE(vine_status [options] [manager] [port])

SECTION(DESCRIPTION)

BOLD(vine_status) displays the status of currently running TaskVine applications.
When run with no options, it queries the global catalog server to list the currently running
TaskVine managers.  When given an address and a port, it queries a manager directly to obtain
more detailed information about tasks and workers.

LIST_BEGIN
LIST_ITEM(Hostname and port number of the application.)
LIST_ITEM(Number of waiting tasks.)
LIST_ITEM(Number of completed tasks.)
LIST_ITEM(Number of connected workers.)
LIST_ITEM(Number of tasks currently being executed.)
LIST_ITEM(Timestamp of when there was last communication with the application.)
LIST_END

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG_LONG(where,expr) Show only TaskVine managers matching this expression.
OPTION_FLAG(Q,statistics)Show summary information about managers. (default)
OPTION_ARG(M,project-name,name)Filter results of -Q for managers matching PARAM(name).
OPTION_FLAG(W,workers)Show details of all workers connected to the manager.
OPTION_FLAG(T,tasks)Show details of all tasks known by the manager.
OPTION_FLAG(A,able-workers)List categories of the given manager, size of largest task, and workers that can run it.
OPTION_FLAG(R,resources)Show available resources for each manager.
OPTION_FLAG_LONG(capacity)Show resource capacities for each manager.
OPTION_FLAG(l,verbose)Long output.
OPTION_ARG(C, catalog, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_ARG(C, catalog, catalog)Set catalog server to PARAM(catalog). Format: HOSTNAME:PORT
OPTION_ARG(d, debug, flag)Enable debugging for the given subsystem. Try -d all as a start.
OPTION_ARG(t, timeout, time)RPC timeout (default=300s).
OPTION_ARG(o, debug-file, file)Send debugging to this file. (can also be :stderr, or :stdout)
OPTION_ARG(O, debug-rotate-max, bytes)Rotate debug file once it reaches this size.
OPTION_FLAG(v,version)Show vine_status version.
OPTION_FLAG(h,help)Show this help message.
OPTIONS_END

SECTION(EXAMPLES)

Without arguments, CODE(vine_status) shows a summary of all of the
projects currently reporting to the default catalog server. Waiting, running,
and complete columns refer to number of tasks:

LONGCODE_BEGIN
$ vine_status
PROJECT            HOST                   PORT WAITING RUNNING COMPLETE WORKERS
shrimp             cclws16.cse.nd.edu     9001     963      37        2      33
crustacea          terra.solar.edu        9000       0    2310    32084     700
LONGCODE_END

With the CODE(-R) option, a summary of the resources available to each manager is shown:

LONGCODE_BEGIN
$ vine_status -R
MANAGER                         CORES      MEMORY          DISK
shrimp                         228        279300          932512
crustacea                      4200       4136784         9049985
LONGCODE_END

With the CODE(--capacity) option, a summary of the resource capacities for each manager is shown:

LONGCODE_BEGIN
$ vine_status --capacity
MANAGER                         TASKS      CORES      MEMORY          DISK
refine                         ???        ???        ???             ???
shrimp                         99120      99120      781362960       1307691584
crustacea                      318911     318911     326564864       326564864
LONGCODE_END

Use the CODE(-W) option to list the workers connected to a particular manager.
Completed and running columns refer to numbers of tasks:

LONGCODE_BEGIN
$ vine_status -W cclws16.cse.nd.edu 9001
HOST                     ADDRESS          COMPLETED RUNNING
mccourt02.helios.nd.edu  10.176.48.102:40         0 4
cse-ws-23.cse.nd.edu     129.74.155.120:5         0 0
mccourt50.helios.nd.edu  10.176.63.50:560         4 4
dirt02.cse.nd.edu        129.74.20.156:58         4 4
cclvm03.virtual.crc.nd.e 129.74.246.235:3         0 0
LONGCODE_END

With the CODE(-T) option, a list of all the tasks submitted to the manager is shown:

LONGCODE_BEGIN
$ vine_status -T cclws16.cse.nd.edu 9001
ID       STATE    PRIORITY HOST                     COMMAND
1000     WAITING         0 ???                      ./rmapper-cs -M fast -M 50bp 1
999      WAITING         0 ???                      ./rmapper-cs -M fast -M 50bp 1
21       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 3
20       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
19       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
18       running         0 cse-ws-35.cse.nd.edu     ./rmapper-cs -M fast -M 50bp 2
...
LONGCODE_END


The CODE(-A) option shows a summary of the resources observed per task
category.

LONGCODE_BEGIN
$ vine_status -A cclws16.cse.nd.edu 9001
CATEGORY        RUNNING    WAITING  FIT-WORKERS  MAX-CORES MAX-MEMORY   MAX-DISK
analysis            216        784           54          4      ~1011      ~3502
merge                20         92           30         ~1      ~4021      21318
default               1         25           54         >1       ~503       >243
LONGCODE_END

With the -A option:
LIST_BEGIN
LIST_ITEM(Running and waiting correspond to number of tasks in the category.)
LIST_ITEM(Fit-workers shows the number of connected workers able to run the largest task in the category.)
LIST_ITEM()max-cores, max-memory, and max-disk show the corresponding largest value in the category.
LIST_END

The value may have the following prefixes:
LIST_BEGIN
LIST_ITEM(No prefix.) The maximum value was manually specified.
LIST_ITEM(~) All the task have run with at most this quantity of resources.
LIST_ITEM(＞) There is at least one task that has used more than this quantity of resources, but the maximum remains unknown.
LIST_END


Finally, the CODE(-l) option shows statistics of the manager in a JSON object:

LONGCODE_BEGIN
$ vine_status -l cclws16.cse.nd.edu 9001
{"categories":[{"max_disk":"3500","max_memory":"1024","max_cores":"1",...
LONGCODE_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_TASK_VINE

FOOTER
