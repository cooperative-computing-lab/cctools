include(manual.h)dnl
HEADER(catalog_history_filter)

SECTION(NAME)
BOLD(catalog_history_filter) - command line tool that filters streamed data from catalog_history_select, but keeps the format the same.

SECTION(SYNOPSIS)
CODE(BOLD(catalog_history_filter [options] [filters]))

SECTION(DESCRIPTION)

BOLD(catalog_history_filter) is a tool to reduce the amount of data returned by catalog_history_select. The arguments define which history results should be output and which should be ignored...

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ITEM(`-static') BOLD(Default:) All filters should operate on static variables. A given series is included if the initialization fields match any filter, otherwise it can be ignored.
OPTION_ITEM(`-dynamic')All filters should operate on dynamic variables. A given series is assumed to be included unless it matches an argument, and at that point it can be ignored. This can consume a lot of memory.
OPTIONS_END

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` filters') Any number of arguments of the form <field><comparison_operator><value> with no spaces in between such as: type=wq_master.
OPTIONS_END

SECTION(EXAMPLES)

To include wq_master or chirp history data:

LONGCODE_BEGIN
% catalog_history_filter type=wq_master type=chirp
LONGCODE_END

To include only wq_master history running version 3.7.3:

LONGCODE_BEGIN
% catalog_history_filter type=wq_master | catalog_history_filter version=3.7.3
LONGCODE_END

To ignore any series which reports, at any point, less than 100000 for the field \'memory_avail\':

LONGCODE_BEGIN
% catalog_history_filter -dynamic memory_avail<100000
LONGCODE_END

To see full results using all catalog history tools:

LONGCODE_BEGIN
% catalog_history_select /data/catalog.history/ 2013-04-15-01-01-01 w1 | catalog_history_filter type=wq_master | catalog_history_plot 3600 SUM.MIN@task_running SUM.AVG@task_running SUM.MAX@task_running
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Catalog History User's Manual,"http://www.nd.edu/~ccl/software/manuals/catalog_history.html")
LIST_ITEM MANPAGE(catalog_history_select,1)
LIST_ITEM MANPAGE(catalog_history_plot,1)
LIST_END

FOOTER

