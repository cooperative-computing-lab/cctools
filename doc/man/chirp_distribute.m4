include(manual.h)dnl
HEADER(chirp_stream_files)

SECTION(NAME)
BOLD(chirp_distributed) - copy a directory from one Chirp server to one or more Chirp server(s).

SECTION(SYNOPSIS)
CODE(BOLD(chirp_distributed [options] PARAM(sourcehost) PARAM(sourcepath) PARAM(host1) PARAM(host2) PARAM(host3) ...))

SECTION(DESCRIPTION)
BOLD(chirp_distributed) is a tool for coping commonly used data across Chirp servers. Data is originated from a Chirp server PARAM(sourcehost) PARAM(sourcepath) and is copied to a given list of Chirp server(s) PARAM(host1) PARAM(host2) PARAM(host3), etc ...
PARA
BOLD(chirp_distributed) is a quick and simple way for replicating a directory from a Chirp server to many Chirp Servers by creating a spanning tree and then transferring data concurrently from host to host using third party transfer. It is faster than manually copying data using BOLD(parrot cp), BOLD(chirp_put) or BOLD(chirp_third_put)
PARA
BOLD(chirp_distributed) also can clean up replicated data using -X option.
SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-X) Delete data from all of the target hosts.
OPTION_ITEM(-D) Show detailed location, time, and performance of each transfer.
OPTION_ITEM(-Y) Show confirmation of successful placements.
OPTION_PAIR(-F,file) Write matrix of failures to this file.
OPTION_PAIR(-T,time) Overall timeout for entire distribution. (default is 3600).
OPTION_ITEM(-R) Randomize order of target hosts given on command line.
OPTION_PAIR(-N,num) Stop after this number of successful copies.
OPTION_PAIR(-t,time) Timeout for for each copy. (default is 3600s)
OPTION_PAIR(-p,num) Maximum number of processes to run at once (default=100)
OPTION_PAIR(-a,mode) Require this authentication mode.
OPTION_PAIR(-d,subsystem) Enable debugging for this subsystem.
OPTION_ITEM(-v) Show program version.
OPTION_ITEM(-h) Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
To copy a directory from server1 to server2, server3, server4 using BOLD(chirp_distributed):
LONGCODE_BEGIN
chirp_distributed server1.somewhere.edu /mydata server2.somewhere.edu server3.somewhere.edu server4.somewhere.edu
LONGCODE_END

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using BOLD(chirp_distributed) and BOLD(chirp_status):
changequote(<!,!>)
LONGCODE_BEGIN
chirp_distributed server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using BOLD(chirp_distributed) and BOLD(chirp_status). However stop when reach 100 copies with -N option:
LONGCODE_BEGIN
chirp_distributed -N 100 server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END

To clean up replicated data using BOLD(chirp_distributed) using -X option:
LONGCODE_BEGIN
chirp_distributed -X server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END
SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(parrot_run,1)
LIST_ITEM MANPAGE(makeflow,1)
LIST_END
