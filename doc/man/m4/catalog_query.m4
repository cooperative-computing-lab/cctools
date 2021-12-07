include(manual.h)dnl
HEADER(catalog_query)

SECTION(NAME)
BOLD(catalog_query) - query records from the catalog server

SECTION(SYNOPSIS)
CODE(catalog_query [--where [expr]] [--catalog [host]] [-d [flag]] [-o [file]] [-O [size]] [-t [timeout]] [-h] )

SECTION(DESCRIPTION)

BOLD(catalog_query) is a tool that queries the catalog server for running services.
The output can be filtered by an arbitrary expression, and displayed in raw JSON
form, or in tabular form. This tool is handy for querying custom record types not handled
by other tools.

SECTION(ARGUMENTS)

OPTIONS_BEGIN
OPTION_ARG_LONG(where, expr) Only records matching this expression will be displayed.
OPTION_ARG_LONG(output, expr) Display this expression for each record.
OPTION_ARG_LONG(catalog, host) Query this catalog host.
OPTION_ARG_LONG(debug, flag) Enable debugging for this subsystem.
OPTION_ARG_LONG(debug-file, file) Send debug output to this file.
OPTION_ARG_LONG(debug-rotate-max, bytes) Rotate debug file once it reaches this size.
OPTION_ARG_LONG(timeout, seconds) Abandon the query after this many seconds.
OPTION_FLAG_LONG(help) Show command options.
OPTIONS_END

SECTION(EXAMPLES)

To show all records in the catalog server:

LONGCODE_BEGIN
% catalog_query
LONGCODE_END

To show all records of other catalog servers:

LONGCODE_BEGIN
% catalog_query --where \'type=="catalog"\'
LONGCODE_END

To show all records of Chirp servers with more than 4 cpus:

LONGCODE_BEGIN
% catalog_query --where \'type=="chirp" && cpus > 4\'
LONGCODE_END

To show all records of WQ applications with name, port, and owner in tabular form:

LONGCODE_BEGIN
% catalog_query --where \'type=="wq_master\" --output name --output port --output owner
LONGCODE_END

To show all records of WQ applications with name, port, and owner as JSON records:

LONGCODE_BEGIN
% catalog_query --where \'type=="wq_master\" --output '{"name":name,"port":port,"owner":owner}'
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
