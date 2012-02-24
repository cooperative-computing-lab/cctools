include(manual.h)dnl
HEADER(makeflow_monitor)

SECTION(NAME)
BOLD(makeflow_monitor) - Makeflow log monitor

SECTION(SYNOPSIS)
CODE(BOLD(makeflow_monitor [options] PARAM(makeflowlog)))

SECTION(DESCRIPTION)
CODE(makeflow_monitor) is simple BOLD(Makeflow) log monitor that displays the
progress and statistics of a workflow based on the provided PARAM(makeflowlog).
Once started, it will continually monitor the specified PARAM(makeflowlogs) for
new events and update the progress display.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(-h)Show this help message and exit.
OPTION_PAIR(-f, format)Output format to emit.
OPTION_PAIR(-t, seconds)Timeout for reading the logs.
OPTION_PAIR(-m, minimum)Mininum number of tasks.
OPTION_ITEM(-S)Sort logs by progress.
OPTION_ITEM(-P)Parse dag for node information.
OPTION_ITEM(-H)Hide finished makeflows.
OPTIONS_END
PARA
Currently, the only supported PARAM(format) is "text", which means
CODE(makeflow_monitor) will display the progress of the workflows directly to
the console.  
PARA
Additionally, the CODE(-P) parameter current does not do anything.

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
Monitor a BOLD(Makeflow) log:
LONGCODE_BEGIN
makeflow_monitor Makeflow.makeflowlog
LONGCODE_END
Monitor multiple BOLD(Makeflow) logs and hide finished workflows:
LONGCODE_BEGIN
makeflow_monitor -H */*.makeflowlog
LONGCODE_END
Monitor multiple BOLD(Makeflow) logs under current directory and only display
currently running workflows with a minimum of 4 tasks:
LONGCODE_BEGIN
find . -name '*.makeflowlog' | xargs makeflow_monitor -m 4 -H 
LONGCODE_END
The example above is useful for hierarchical workflows.

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_MAKEFLOW

FOOTER

