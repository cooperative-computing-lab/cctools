include(manual.h)dnl
HEADER(chirp_stream_files)

SECTION(NAME)
BOLD(chirp_distribute) - copy a directory from one Chirp server to one or more Chirp server(s).

SECTION(SYNOPSIS)
CODE(BOLD(chirp_distribute [options] PARAM(sourcehost) PARAM(sourcepath) PARAM(host1) PARAM(host2) PARAM(host3) ...))

SECTION(DESCRIPTION)
BOLD(chirp_distribute) is a tool for coping commonly used data across Chirp servers. Data is originated from a Chirp server PARAM(sourcehost) PARAM(sourcepath) and is copied to a given list of Chirp server(s) PARAM(host1) PARAM(host2) PARAM(host3), etc ...
PARA
BOLD(chirp_distribute) is a quick and simple way for replicating a directory from a Chirp server to many Chirp Servers by creating a spanning tree and then transferring data concurrently from host to host using third party transfer. It is faster than manually copying data using BOLD(parrot cp), BOLD(chirp_put) or BOLD(chirp_third_put)
PARA
BOLD(chirp_distribute) also can clean up replicated data using -X option.
SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-a,auth,flag)Require this authentication mode.
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_ITEM(`-D, --info-transfer')Show detailed location, time, and performance of each transfer.
OPTION_TRIPLET(-F,failures-file,file)Write matrix of failures to this file.
OPTION_TRIPLET(-i,tickets,files)Comma-delimited list of tickets to use for authentication.
OPTION_TRIPLET(-N, copies-max,num)Stop after this number of successful copies.
OPTION_TRIPLET(-p,jobs,num)Maximum number of processes to run at once (default=100)
OPTION_ITEM(`-R, --randomize-hosts')Randomize order of target hosts given on command line.
OPTION_TRIPLET(-t,timeout,time)Timeout for for each copy. (default is 3600s)
OPTION_TRIPLET(-T,timeout-all,time)Overall timeout for entire distribution. (default is 3600).
OPTION_ITEM(`-v, --verbose')Show program version.
OPTION_ITEM(`-X, --delete-target')Delete data from all of the target hosts.
OPTION_ITEM(`-Y, --info-success')Show confirmation of successful placements.
OPTION_ITEM(`-h, --help')Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)
To copy a directory from server1 to server2, server3, server4 using BOLD(chirp_distribute):
LONGCODE_BEGIN
chirp_distribute server1.somewhere.edu /mydata server2.somewhere.edu server3.somewhere.edu server4.somewhere.edu
LONGCODE_END

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using BOLD(chirp_distribute) and BOLD(chirp_status):
changequote(<!,!>)
LONGCODE_BEGIN
chirp_distribute server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using BOLD(chirp_distribute) and BOLD(chirp_status). However stop when reach 100 copies with -N option:
LONGCODE_BEGIN
chirp_distribute -N 100 server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END

To clean up replicated data using BOLD(chirp_distribute) using -X option:
LONGCODE_BEGIN
chirp_distribute -X server1.somewhere.edu /mydata \`chirp_status -s\`
LONGCODE_END
SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER

