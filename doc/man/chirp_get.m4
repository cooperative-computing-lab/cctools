include(manual.h)dnl
HEADER(chirp_get)

SECTION(NAME)
BOLD(chirp_get) - get a single file from a Chirp server to local machine

SECTION(SYNOPSIS)
CODE(BOLD(chirp_get [options] PARAM(hostname[:port]) PARAM(remotefile) PARAM(localfile)))

SECTION(DESCRIPTION)

BOLD(chirp_get) is a tool for copying a single file from a Chirp server to local storage.
PARA
BOLD(chirp_get) is a quick and simple way to copy a remote file given PARAM(hostname[:port]) PARAM(path) and write it to a local file PARAM(localfile)
PARA
BOLD(chirp_get) also can stream data which can be useful in a shell pipeline.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-a,auth,flag)Require this authentication mode.
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem.
OPTION_TRIPLET(-t,timeout,time)Timeout for failure. (default is 3600s)
OPTION_TRIPLET(-i,tickets,files)Comma-delimited list of tickets to use for authentication.
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

To copy a single remote file using BOLD(chirp_get):

LONGCODE_BEGIN
% chirp_get server1.somewhere.edu /mydata/mydata.dat /tmp/mydata.dat
LONGCODE_END

To get, while at the same time, untar a single remote archive file using BOLD(chirp_get):

LONGCODE_BEGIN
% chirp_get myhost.somewhere.edu /mydata/archive.tar.gz - | tar xvzf
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER

