include(manual.h)dnl
HEADER(deltadb_query)

SECTION(NAME)
BOLD(deltadb_query) - query historical data stored by the catalog server.

SECTION(SYNOPSIS)
CODE(deltadb_query --db [source_directory] --from [starttime] --to [endtime] [--filter [expr]] [--where [expr]] [--output [expr]])

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
OPTION_ARG_LONG(--db, path) Query this database directory.
OPTION_ARG_LONG(--file, path) Query the data stream in this file.
OPTION_ARG_LONG(--from, time) (required) The starting date and time of the query in an absolute time like "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD" or a relative time like 5s, 5m, 5h, 5d to indicate five seconds, minutes, hours, or days ago, respectively.
OPTION_ARG_LONG(--to, time) The ending time of the query, in the same format as the --from option.  If omitted, the current time is assumed.
OPTION_ARG_LONG(--every, interval) The intervals at which output should be produced, like 5s, 5m, 5h, 5d to indicate five seconds, minutes, hours, or days ago, respectively.
OPTION_FLAG_LONG(--epoch), Causes the output to be expressed in integer Unix epoch time, instead of a formatted time.
OPTION_ARG_LONG(--filter, expr) (multiple) If given, only records matching this expression will be processed.  Use --filter to apply expressions that do not change over time, such as the name or type of a record.
OPTION_ARG_LONG(--where, expr)  (multiple) If given, only records matching this expression will be displayed.  Use --where to apply expressions that may change over time, such as load average or storage space consumed.
OPTION_ARG_LONG(--output, expr) (multiple) Display this expression on the output.
OPTIONS_END

SECTION(JX QUERY LANGUAGE)

The --filter, --where, and --output options all make use of the JX query language,
which is described URL(here,http://cctools.readthedocs.io/jx).

In addition, the --output clauses may contain one of the following reduction functions:

LIST_BEGIN
LIST_ITEM(COUNT) Give the count of items in the set.
LIST_ITEM(SUM) Give the sum of the values in the set.
LIST_ITEM(FIRST) Give the first value encountered in the set.
LIST_ITEM(LAST) Give the last value encountered in the set.
LIST_ITEM(MIN) Give the minimum value in the set.
LIST_ITEM(MAX) Give the maximum value in the set.
LIST_ITEM(AVERAGE) Give the average value of the set.
LIST_ITEM(UNIQUE) Give a list of unique values in the set.
LIST_END

(The UNIQUE function can be applied to any record type, while the other reduction functions assume numeric values.)

By default, reductions are computed spatially.  This means the reduction is computed across the set of 
records available at each periodic time step.  For example, MAX(count) gives the (single) maximum value of
count seen across all records.  A single value MAX(count) will be displayed at each output step.

If the prefix TIME_ is added to a reduction, then the reduction is computed over time for each record.
For example, TIME_MAX(count) computes the maximum value of count over each time interval, for each record.
TIME_MAX(count) will be given for each record at each output step.

Finally, if the prefix GLOBAL is added to a reduction, then it will be computed for all records
across all time intervals.  For example, GLOBAL_MAX(count) will display the maximum value of count
seen for any record at any time.  A single value GLOBAL_MAX(count) will be display at each output step.

For example, this:

LONGCODE_BEGIN
--output 'UNIQUE(name)' --every 7d
LONGCODE_END

will display all of the names in the database, at seven day intervals.  However, it will not display records that are created and deleted between those seven day intervals.  In comparison, this:

LONGCODE_BEGIN
--output 'GLOBAL_UNIQUE(name)' --every 7d
LONGCODE_END

will display all of the names encountered over the last seven days, at seven day intervals.

SECTION(EXAMPLES)

To show 1 week worth of history starting on 15 April 2013:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-04-15 --to 2015-04-22
LONGCODE_END

To show all history after 1 March 2013:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01
LONGCODE_END

To show the names of fred's chirp servers where load5 exceeds 2.0:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01 --filter 'owner=="fred" && type=="chirp"' --where 'load5>2.0' --output name --output load5
LONGCODE_END

To show the average load of all servers owned by fred at one hour intervals:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2013-03-01 --filter 'owner=="fred" && type=="chirp"' --output 'AVERAGE(load5)' --every 1h
LONGCODE_END

To show the project names of all Work Queue applications running at a given time:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --at 2020-12-01 --filter 'type=="wq_master"' --output 'UNIQUE(project)'
LONGCODE_END

To show the number of managers, tasks, and cores in use for all Work Queue applications:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2020-01-01 --to 2021-01-01 --filter 'type=="wq_master"' --output 'COUNT(name)' --output 'SUM(tasks_running)' -- output 'SUM(cores_inuse)'
LONGCODE_END

The raw event output of a query can be saved to a file, and then queried using the --file option, which can accelerate operations on reduced data.  For example:

LONGCODE_BEGIN
% deltadb_query --db /data/catalog.history --from 2020-01-01 --to 2021-01-01 --filter 'type=="wq_master"' > wq.data
% deltadb_query --file wq.data --output 'COUNT(name)' --output 'SUM(tasks_running)' -- output 'SUM(cores_inuse)'
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
