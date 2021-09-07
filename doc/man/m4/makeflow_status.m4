include(manual.h)dnl
HEADER(makeflow_status)

SECTION(NAME)
BOLD(makeflow_status) - command line tool retrieving the status of makeflow programs.

SECTION(SYNOPSIS)
CODE(makeflow_status [options])

SECTION(DESCRIPTION)

BOLD(makeflow_status) retrieves the status of makeflow programs and prints out a report to BOLD(stdout). By using flags, users can filter out certain responses, such as only finding reports of a certain projet, or a certain project owner.


SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG(M, project, project)The project on which to filter results.
OPTION_ARG(u, username, user)The owner on which to filter results.
OPTION_ARG(s, server, server)The catalog server to retrieve the reports from.
OPTION_ARG(p, port, port)The port to contact the catalog server on.
OPTION_ARG(t, timeout, time)Set remote operation timeout.
OPTION_FLAG(h,help)Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(CATALOG_HOST) The catalog server to retrieve reports from (same as CODE(-s)).)
LIST_ITEM(CODE(CATALOG_PORT) The port to contact the catalog server on (same as CODE(-p)).)
LIST_END

SECTION(EXIT STATUS)
On success, returns 0 and prints out the report to stdout.

SECTION(EXAMPLES)

Retrieving reports related to project "awesome"

LONGCODE_BEGIN
% makeflow_status -M awesome
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
