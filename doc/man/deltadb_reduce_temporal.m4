include(manual.h)dnl
HEADER(deltadb_reduce_temporal)

SECTION(NAME)
BOLD(deltadb_reduce_temporal) - command line tool that aggregates attribute values (from standard input) over a specified time span.

SECTION(SYNOPSIS)
CODE(BOLD(deltadb_reduce_temporal [time span] [arguments]))

SECTION(DESCRIPTION)

BOLD(deltadb_reduce_temporal) is a tool to summarize the data when multiple values exist over a specified time span for a given object and attribute. It first aggregates attribute values over the time span, and then reduces them to a single value which represents the time span based on the reducer in the argument for that attribute.

BOLD(deltadb) (prefix 'deltadb_') is a collection of tools designed to operate on data in the format stored by the catalog server (a log of object changes over time). They are designed to be piped together to perform customizable queries on the data. A paper entitled DeltaDB describes the operation of the tools in detail (see reference below).

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` time span') The time span span over which attribute values will be reduced. Accepts the following formats; s\#\# for seconds, m\#\# for minutes, h\#\# for hours, d\#\# for days, y\#\# for years. (m15 means 15 minute time spans)
OPTION_ITEM(` arguments') Any number of arguments of the form <field>,<reduction_operator> such as: tasks_running,MAX. Acceptable reduction operators are currently MIN, MAX, AVERAGE, FIRST, and LAST.
OPTIONS_END

SECTION(EXAMPLES)

To include the largest number of workers each day:

LONGCODE_BEGIN
% deltadb_reduce_temporal d1 workers,MAX
LONGCODE_END


To see full results using a chain of multiple deltadb tools:

LONGCODE_BEGIN
% deltadb_collect /data/catalog.history 2013-02-1@00:00:00 d7 | \\
% deltadb_select_static  type=wq_master | \\
% deltadb_reduce_temporal m15 workers,MAX task_running,MAX tasks_running,MAX | \\
% deltadb_reduce_spatial name,CNT workers.MAX,SUM task_running.MAX,SUM tasks_running.MAX,SUM | \\
% deltadb_pivot name.CNT workers.MAX.SUM task_running.MAX.SUM tasks_running.MAX.SUM
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(DeltaDB User's Manual,"http://www.nd.edu/~ccl/software/manuals/deltadb.html")
LIST_ITEM LINK(DeltaDB paper,"http://www.nd.edu/~ccl/research/papers/pivie-deltadb-2014.pdf")
LIST_ITEM MANPAGE(deltadb_select_collect,1)
LIST_ITEM MANPAGE(deltadb_select_static,1)
LIST_ITEM MANPAGE(deltadb_select_dynamic,1)
LIST_ITEM MANPAGE(deltadb_select_complete,1)
LIST_ITEM MANPAGE(deltadb_project,1)
LIST_ITEM MANPAGE(deltadb_reduce_spatial,1)
LIST_ITEM MANPAGE(deltadb_pivot,1)
LIST_END

FOOTER

