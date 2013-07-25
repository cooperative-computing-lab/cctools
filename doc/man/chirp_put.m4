include(manual.h)dnl
HEADER(chirp_put)

SECTION(NAME)
BOLD(chirp_put) - copy a single file from local machine to a Chirp server

SECTION(SYNOPSIS)
CODE(BOLD(chirp_put [options] PARAM(localfile) PARAM(hostname[:port]) PARAM(remotefile)))

SECTION(DESCRIPTION)

BOLD(chirp_put) is a tool for copying a single file from local storage to a Chirp server.
PARA
BOLD(chirp_put) is a quick and simple way to copy a single local file PARAM(localfile) to a remote file given PARAM(hostname[:port]) PARAM(path)

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-a,auth, flag)Require this authentication mode.
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-b,block-size,size)Set transfer buffer size. (default is 65536 bytes).
OPTION_ITEM(`-f, --follow')Follow input file like tail -f.
OPTION_TRIPLET(-i,tickets,files)Comma-delimited list of tickets to use for authentication.
OPTION_TRIPLET(-t,timeout, time)Timeout for failure. (default is 3600s)
OPTION_ITEM(`-v, --version')Show program version.
OPTION_ITEM(`-h, --help')Show help text.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To copy a single local file using BOLD(chirp_put):

LONGCODE_BEGIN
% chirp_put /tmp/mydata.dat server1.somewhere.edu /mydata/mydata.dat
LONGCODE_END

When copying big data files that take longer than 3600s to copy, using BOLD(chirp_put) with option -t time to make sure BOLD(chirp_put) have enough time to finish:

LONGCODE_BEGIN
% chirp_put -t 36000 /tmp/mybigdata.dat server1.somewhere.edu /mydata/mybigdata.dat
LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER

