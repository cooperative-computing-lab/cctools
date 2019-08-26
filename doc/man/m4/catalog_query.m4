include(manual.h)dnl
HEADER(catalog_query)

SECTION(NAME)
BOLD(catalog_query) - query records from the catalog server

SECTION(SYNOPSIS)
CODE(BOLD(catalog_query [--where [expr]] [--catalog [host]] [-d [flag]] [-o [file]] [-O [size]] [-t [timeout]] [-h] ))

SECTION(DESCRIPTION)

BOLD(catalog_query) is a tool that queries a catalog server for raw
JSON records.  The records can be filtered by an optional --where expression.
This tool is handy for querying custom record types not handled
by other tools.

SECTION(ARGUMENTS)

OPTIONS_BEGIN
OPTION_ITEM(--where expr) Only records matching this expression will be displayed.
OPTION_ITEM(--catalog host) Query this catalog host.
OPTION_ITEM(--debug flag) Enable debugging for this subsystem.
OPTION_ITEM(--debug-file file) Send debug output to this file.
OPTION_ITEM(--debug-rotate-max bytes) Rotate debug file once it reaches this size.
OPTION_ITEM(--timeout seconds) Abandon the query after this many seconds.
OPTION_ITEM(--help) Show command options.
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

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CATALOG

FOOTER
