include(manual.h)dnl
HEADER(deltadb_pivot)

SECTION(NAME)
BOLD(deltadb_pivot) - command line tool that returns deltadb data in a more easily plottable format.

SECTION(SYNOPSIS)
CODE(BOLD(deltadb_pivot [arguments]))

SECTION(DESCRIPTION)

BOLD(deltadb_pivot) is a tool which returns a column for each attribute in the arguments, where each row contains the value of that attribute for each object and each timestamp in the deltadb stream.

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` arguments') The attributes in the input that should be included as columns in the output.
OPTIONS_END

SECTION(EXAMPLES)

To show the object name for each object at each timestamp:

LONGCODE_BEGIN
% deltadb_pivot name
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
LIST_ITEM(LINK(The Cooperative Computing Tools,"http://ccl.cse.nd.edu/software/manuals"))
LIST_ITEM(LINK(DeltaDB User's Manual,"http://ccl.cse.nd.edu/software/manuals/deltadb.html"))
LIST_ITEM(LINK(DeltaDB paper,"http://ccl.cse.nd.edu/research/papers/pivie-deltadb-2014.pdf"))
LIST_ITEM(MANPAGE(deltadb_select_collect,1))
LIST_ITEM(MANPAGE(deltadb_select_static,1))
LIST_ITEM(MANPAGE(deltadb_select_dynamic,1))
LIST_ITEM(MANPAGE(deltadb_select_complete,1))
LIST_ITEM(MANPAGE(deltadb_project,1))
LIST_ITEM(MANPAGE(deltadb_reduce_temporal,1))
LIST_ITEM(MANPAGE(deltadb_reduce_spatial,1))
LIST_ITEM(MANPAGE(deltadb_pivot,1))
LIST_END

FOOTER
