include(manual.h)dnl
HEADER(chirp_fuse)

SECTION(NAME)
BOLD(chirp_fuse) - create a CODE(FUSE) mount point with virtual access to remote chirp servers

SECTION(SYNOPSIS)
CODE(chirp_fuse [options] PARAM(mount path))

SECTION(DESCRIPTION)

PARA
You can attach to a Chirp filesystems by using the FUSE package to attach Chirp
as a kernel filesystem module. Unlike MANPAGE(parrot_run,1), this requires
superuser privileges to install the FUSE package, but will likely work more
reliably on a larger number of programs and environments. Using FUSE allows for
transparent access to a Chirp server via the local filesystem. This allows user
applications to use a Chirp store unmodified.

PARA
Once you have installed and permissions to use FUSE, simply run chirp_fuse with
the name of a directory on which the filesystem should be mounted.

PARA
For complete details with examples, see the
LINK(Chirp User's Manual,http://ccl.cse.nd.edu/software/manuals/chirp.html).

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ARG(a,auth,flag) Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
OPTION_ARG(b,block-size,bytes)Block size for network I/O. (default is 65536s)
OPTION_ARG(d,debug,flag)Enable debugging for this subsystem.
OPTION_FLAG(D,no-optimize)Disable small file optimizations such as recursive delete.
OPTION_FLAG(f,foreground)Run in foreground for debugging.
OPTION_ARG(i,tickets,files)Comma-delimited list of tickets to use for authentication.
OPTION_ARG(m,mount-options,option)Pass mount option to FUSE. Can be specified multiple times.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ARG(t,timeout,timeout)Timeout for network operations. (default is 60s)
OPTION_FLAG(v,version)Show program version.
OPTION_FLAG(h,help)Give help information.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(CHIRP_CLIENT_TICKETS)) Comma delimited list of tickets to authenticate with (same as CODE(-i)).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To create and use a CODE(FUSE) mount point for access to remote chirp servers:

LONGCODE_BEGIN
% chirp_fuse /tmp/chirp-fuse &
% cd /tmp/chirp-fuse
% ls
% cd host:port
% cat foo/bar
% exit
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_CHIRP

FOOTER
