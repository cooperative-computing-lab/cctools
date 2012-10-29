include(manual.h)dnl
HEADER(chirp_status)

SECTION(NAME)
BOLD(chirp_status) - get current status of a one or more Chirp server(s)

SECTION(SYNOPSIS)
CODE(BOLD(chirp_status [options] PARAM(nane) PARAM(value)))

SECTION(DESCRIPTION)
BOLD(chirp_status) is a tool for checking status of Chirp server(s).
PARA
BOLD(chirp_status) can look up Chirp server(s) using type, name, port, owner and version.
PARA
BOLD(chirp_status) by default lists type, name, port, owner, version, total and available storage of Chirp server(s)
PARA
When using CODE(chirp_status) with long form option (-l), it lists additional information such as average load, available memory, operating system, up time, etc...

SECTION(OPTIONS)
OPTION_PAIR(-c,host)Query the catalog on this host.
OPTION_PAIR(-d,subsystem)Enable debugging for this subsystem.
OPTION_PAIR(-o,file)Send debugging output to this file.
OPTION_PAIR(-O,bytes)Rotate file once it reaches this size.
OPTION_PAIR(-A,size)Only show servers with this space available. (example: -A 100MB).
OPTION_PAIR(-t,time)Timeout.
OPTION_ITEM(-s)Short output.
OPTION_ITEM(-l)Long output.
OPTION_ITEM(-T)Totals output.
OPTION_ITEM(-v)Show program version.
OPTION_ITEM(-h)Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
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
% chirp_status server1.somewhere.edu
LONGCODE_END

To show all details of a single server:

LONGCODE_BEGIN
% chirp_status -l server1.somewhere.edu
LONGCODE_END

To show status of Chirp servers which belong to a particular owner using BOLD(chirp_status):

LONGCODE_BEGIN
% chirp_status owner ownername
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

