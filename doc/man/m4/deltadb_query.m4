include(manual.h)dnl
HEADER(deltadb_query)

SECTION(NAME)
BOLD(deltadb_query) - query historical data stored by the catalog server.

SECTION(SYNOPSIS)
CODE(BOLD(deltadb_query --db [source_directory] --from [starttime] --to [endtime] [--filter [expr]] [--where [expr]] [--output [expr]]))

SECTION(DESCRIPTION)

BOLD(deltadb_query) is a tool that examines and displays historical data of the catalog server.
(If given the -H option, the catalog server will record historical data in a format
known as DeltaDB, hence the name of this tool.)
The results can be displayed as a stream of time-ordered updates
or as summaries of properties across all records over time.
This is useful for reporting, for example, the total resources and clients
served by a large collection of servers over the course of a year.

A paper entitled DeltaDB describes the operation of the tools in detail (see reference below).

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(--db path) Query this database directory.
OPTION_ITEM(--file path) Query the data stream in this file.
OPTION_ITEM(--from time) (required) The starting date and time of the query in an absolute time like "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" or a relative time like 5s, 5m, 5h, 5d to indicate five seconds, minutes, hours, or days ago, respectively.
OPTION_ITEM(--to time) The ending time of the query, in the same format as the --from option.  If omitted, the current time is assumed.
OPTION_ITEM(--every interval) The intervals at which output should be produced, like 5s, 5m, 5h, 5d to indicate five seconds, minutes, hours, or days ago, respectively.
OPTION_ITEM(--epoch) Causes the output to be expressed in integer Unix epoch time, instead of a formatted time.
OPTION_ITEM(--filter expr) (multiple) If given, only records matching this expression will be processed.  Use --filter to apply expressions that do not change over time, such as the name or type of a record.
OPTION_ITEM(--where expr)  (multiple) If given, only records matching this expression will be displayed.  Use --where to apply expressions that may change over time, such as load average or storage space consumed.
OPTION_ITEM(--output expr) (multiple) Display this expression on the output.
OPTIONS_END

SECTION(EXAMPLES)

To show 1 week worth of history starting on 15 April 2013:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-04-15 --to 2015-04-22
LONGCODE_END

To show all history after 1 March 2013:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01
LONGCODE_END

To show the names of fred's servers where load5 exceeds 2.0:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01 --filter 'owner=="fred"' --where 'load5>2.0' --output name --output load5
LONGCODE_END

To show the average load of all servers owned by fred at one hour intervals:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01 --filter 'owner=="fred"' --output 'AVERAGE(load5)' --every 1h
LONGCODE_END

The raw event output of a query can be saved to a file, and then queried using the --file option, which can accelerate operations on reduced data.  For example:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2014-01-01 --to 2015-01-01 --filter 'type=="wq_manager"' > wq.data
% deltadb_query --file wq.data --from 2014-01-01 --output 'COUNT(name)' --output 'MAX(tasks_running)'
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
