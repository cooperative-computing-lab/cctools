include(manual.h)dnl
HEADER(chirp_status)

SECTION(NAME)
BOLD(chirp_status) - get current status of a one or more Chirp server(s)

SECTION(SYNOPSIS)
CODE(chirp_status [options] PARAM(nane) PARAM(value))

SECTION(DESCRIPTION)
BOLD(chirp_status) is a tool for checking status of Chirp server(s).
PARA
BOLD(chirp_status) can look up Chirp server(s) using type, name, port, owner and version.
PARA
BOLD(chirp_status) by default lists type, name, port, owner, version, total and available storage of Chirp server(s)
PARA
When using CODE(chirp_status) with long form option (-l), it lists additional information such as average load, available memory, operating system, up time, etc...

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG_LONG(where,expr) Show only servers matching this expression.
OPTION_ARG(c,catalog,host)Query the catalog on this host.
OPTION_ARG(A,server-space,size)Only show servers with this space available. (example: -A 100MB).
OPTION_ARG_LONG(server-project,name)Only servers with this project name.
OPTION_ARG(t,timeout,time)Timeout.
OPTION_FLAG(s,brief)Short output.
OPTION_FLAG(l,verbose)Long output.
OPTION_FLAG(T,totals)Totals output.
OPTION_FLAG(v,version)Show program version.
OPTION_ARG(d,debug,flag)Enable debugging for this subsystem.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ARG(O,debug-rotate-max,bytes)Rotate file once it reaches this size.
OPTION_FLAG(h,help)Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To show status of all available Chirp servers using BOLD(chirp_status):

LONGCODE_BEGIN
% chirp_status
LONGCODE_END

To show status of a particular Chirp server:

LONGCODE_BEGIN
% chirp_status --where 'name=="server1.somewhere.edu"'
LONGCODE_END

To show status of Chirp servers which belong to a particular owner:

LONGCODE_BEGIN
% chirp_status --where 'owner=="fred"'
LONGCODE_END

To show all details in JSON format:

LONGCODE_BEGIN
% chirp_status --long
LONGCODE_END

To show aggregate status of all Chirp servers using  BOLD(chirp_status):

LONGCODE_BEGIN
% chirp_status -T
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER
