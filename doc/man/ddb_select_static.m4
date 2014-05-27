include(manual.h)dnl
HEADER(ddb_select_static)

SECTION(NAME)
BOLD(ddb_select_static) - command line tool that removes objects from a stream of delta logs (standard input), but keeps the format the same.

SECTION(SYNOPSIS)
CODE(BOLD(ddb_select_static [arguments]))

SECTION(DESCRIPTION)

BOLD(ddb_select_static) is a tool to remove objects from the log. The arguments must use attributes which are defined in the initialization of a new object, otherwise they will be removed. The tools ddb_select_dynamic and ddb_select_complete can be used for attributes that are not static.

BOLD(deltadb) (prefix 'ddb_') is a collection of tools designed to operate on data in the format stored by the catalog server (a log of object changes over time). They are designed to be piped together to perform customizable queries on the data. A paper entitled DeltaDB describes the operation of the tools in detail (see reference below).

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` arguments') Any number of arguments of the form <field><comparison_operator><value> with no spaces in between such as: type=wq_master.
OPTIONS_END

SECTION(EXAMPLES)

To include wq_master or chirp history data:

LONGCODE_BEGIN
% ddb_select_static type=wq_master type=chirp
LONGCODE_END

To include only wq_master history running version 3.7.3:

LONGCODE_BEGIN
% ddb_select_static type=wq_master | \\
% ddb_select_static version=3.7.3
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
LIST_ITEM MANPAGE(ddb_select_dynamic,1)
LIST_ITEM MANPAGE(ddb_select_complete,1)
LIST_ITEM MANPAGE(ddb_project,1)
LIST_ITEM MANPAGE(ddb_reduce_temporal,1)
LIST_ITEM MANPAGE(ddb_reduce_spatial,1)
LIST_ITEM MANPAGE(ddb_pivot,1)
LIST_END

FOOTER

