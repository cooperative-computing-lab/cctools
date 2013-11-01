include(manual.h)dnl
HEADER(catalog_history_plot)

SECTION(NAME)
BOLD(catalog_history_plot.py) - command line tool that returns easily plottable data from the output of catalog_history_select or catalog_history_filter.

SECTION(SYNOPSIS)
CODE(BOLD(catalog_history_plot.py [granularity] [summaries]))

SECTION(DESCRIPTION)

BOLD(catalog_history_plot) is a tool that returns the status (or checkpoint) for a specified start time, and all following log data until a specified end time...

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` granularity') The number of seconds between each summarized value in the output.
OPTION_ITEM(` summaries') Any number of arguments specifying what status information should be summarized and how to do so. These arguments start with an interseries aggregation operator, followed by an intraseries aggregation operator, followed by the field to evaluate. For example SUM.AVG@memory_avail, says to look at the values for the memory_avail field, average all values for a series that occured within the specified 'grain', and sum the series averages to obtain a single value for that 'grain' of time. Available intraseries operators are: MAX,MIN,AVG,FIRST,LAST,COUNT,INC,LIST. Available interseries operators are: SUM. .
OPTIONS_END

SECTION(EXAMPLES)

To show the distribution for task_running values within 1 hour time periods:

LONGCODE_BEGIN
% catalog_history_plot.py 3600 SUM.MIN@task_running SUM.AVG@task_running SUM.MAX@task_running
LONGCODE_END

To show the distribution of daily values describing memory:

LONGCODE_BEGIN
% catalog_history_plot.py 86400 SUM.MAX@memory_total SUM.AVG@memory_avail SUM.MIN@minfree
LONGCODE_END

To see full results using all catalog history tools:

LONGCODE_BEGIN
% catalog_history_select.py /data/catalog.history/ 2013-04-15-01-01-01 w1 | catalog_history_filter.py type=wq_master | catalog_history_plot.py 3600 SUM.MIN@task_running SUM.AVG@task_running SUM.MAX@task_running
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Catalog History User's Manual,"http://www.nd.edu/~ccl/software/manuals/catalog_history.html")
LIST_ITEM MANPAGE(catalog_history_select,1)
LIST_ITEM MANPAGE(catalog_history_filter,1)
LIST_END

FOOTER

