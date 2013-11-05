include(manual.h)dnl
HEADER(catalog_history_select)

SECTION(NAME)
BOLD(catalog_history_select) - command line tool that returns catalog history for a specified time period.

SECTION(SYNOPSIS)
CODE(BOLD(catalog_history_select [source_directory] [starting_point] [ending_point_or_duration]))

SECTION(DESCRIPTION)

BOLD(catalog_history_select) is a tool that returns the status (or checkpoint) for a specified start time, and all following log data until a specified end time. Datetimes are viewed as representative of local time...

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` source_directory') Expects a directory containing the catalog data sorted into sub-directories based on year.
OPTION_ITEM(` starting_point') Expects the start time to be a timestamp or date and time in the format YYYY-MM-DD-HH-MM-SS.
OPTION_ITEM(` ending_point_or_duration') Expects a timestamp, duration, or date and time of the same format YYYY-MM-DD-HH-MM-SS. The duration can be in seconds (+180 or s180), minutes (m45), hours (h8), days (d16), weeks (w3), or years (y2).
OPTIONS_END

SECTION(EXAMPLES)

To show 1 week worth of history starting on 15 April 2013:

LONGCODE_BEGIN
% catalog_history_select /data/catalog.history 2013-04-15-01-01-01 w1
LONGCODE_END

To show all history after 1 March 2013:

LONGCODE_BEGIN
% catalog_history_select /data/catalog.history 2013-03-01-01-01
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
LIST_ITEM MANPAGE(catalog_history_filter,1)
LIST_ITEM MANPAGE(catalog_history_plot,1)
LIST_END

FOOTER

