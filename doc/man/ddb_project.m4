include(manual.h)dnl
HEADER(ddb_project)

SECTION(NAME)
BOLD(ddb_project) - command line tool that defines desired attributes from a stream of delta logs (standard input), removing the remaining attributes, but no objects.

SECTION(SYNOPSIS)
CODE(BOLD(ddb_project [attributes]))

SECTION(DESCRIPTION)

BOLD(ddb_project) is a tool to remove object attributes from the streaming data log. The arguments list the attributes that should remain and the `key' attribute always remains in the output.

BOLD(deltadb) (prefix 'ddb_') is a collection of tools designed to operate on data in the format stored by the catalog server (a log of object changes over time). They are designed to be piped together to perform customizable queries on the data. A paper entitled DeltaDB describes the operation of the tools in detail (see reference below).

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` attributes') Any number of arguments, such as: name status field1 field2.
OPTIONS_END

SECTION(EXAMPLES)

To remove all but the name attribute (and the implied key attribute):

LONGCODE_BEGIN
% ddb_project name
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
LIST_ITEM MANPAGE(ddb_reduce_temporal,1)
LIST_ITEM MANPAGE(ddb_reduce_spatial,1)
LIST_ITEM MANPAGE(ddb_pivot,1)
LIST_END

FOOTER

