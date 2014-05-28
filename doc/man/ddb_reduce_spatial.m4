include(manual.h)dnl
HEADER(ddb_reduce_spatial)

SECTION(NAME)
BOLD(ddb_reduce_spatial) - command line tool that summarizes attribute values (from standard input) over all objects.

SECTION(SYNOPSIS)
CODE(BOLD(ddb_reduce_spatial [arguments]))

SECTION(DESCRIPTION)

BOLD(ddb_reduce_spatial) is a tool to summarize the data when multiple objects have matching attributes. A single object with aggregated attribute values is returned for each timestamp.

BOLD(deltadb) (prefix 'ddb_') is a collection of tools designed to operate on data in the format stored by the catalog server (a log of object changes over time). They are designed to be piped together to perform customizable queries on the data. A paper entitled DeltaDB describes the operation of the tools in detail (see reference below).

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` arguments') Any number of arguments of the form <field>,<reduction_operator> such as: tasks_running,MAX. Acceptable reduction operators are currently MIN, MAX, AVERAGE, FIRST, and LAST. If preceded by a temporal reduction, the field name is likely to include a temporal reduction desciptor which would require the argument to look something like this: workers.MAX,SUM
OPTIONS_END

SECTION(EXAMPLES)

To find the total sum of memory available at each timestamp:

LONGCODE_BEGIN
% ddb_reduce_spatial d1 memory_avail,SUM
LONGCODE_END


To see full results using a chain of multiple deltadb tools:

LONGCODE_BEGIN
% ddb_collect /data/catalog.history 2013-02-1@00:00:00 d7 | \\
% ddb_select_static  type=wq_master | \\
% ddb_reduce_temporal m15 workers,MAX task_running,MAX tasks_running,MAX | \\
% ddb_reduce_spatial name,CNT workers.MAX,SUM task_running.MAX,SUM tasks_running.MAX,SUM | \\
% ddb_pivot name.CNT workers.MAX.SUM task_running.MAX.SUM tasks_running.MAX.SUM
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(DeltaDB User's Manual,"http://www.nd.edu/~ccl/software/manuals/deltadb.html")
LIST_ITEM LINK(DeltaDB paper,"http://www.nd.edu/~ccl/research/papers/pivie-deltadb-2014.pdf")
LIST_ITEM MANPAGE(ddb_select_collect,1)
LIST_ITEM MANPAGE(ddb_select_static,1)
LIST_ITEM MANPAGE(ddb_select_dynamic,1)
LIST_ITEM MANPAGE(ddb_select_complete,1)
LIST_ITEM MANPAGE(ddb_project,1)
LIST_ITEM MANPAGE(ddb_reduce_temporal,1)
LIST_ITEM MANPAGE(ddb_pivot,1)
LIST_END

FOOTER

