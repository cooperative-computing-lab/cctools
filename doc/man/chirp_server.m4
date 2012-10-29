include(manual.h)dnl
HEADER(chirp_server)

SECTION(NAME)
BOLD(chirp_server) - create a Chirp user-level filesystem

SECTION(SYNOPSIS)
CODE(BOLD(chirp_server [options]))

SECTION(DESCRIPTION)

PARA
Starts a Chirp server which allows the sharing of data with friends and
colleagues without requiring any administrator privileges.  Chirp provides an
RPC network interface to a "backend" filesystem which can be the local
filesystem or even the Hadoop HDFS filesystem. Chirp supports flexible and
robust ACL management of data.

PARA
For complete details with examples, see the LINK(Chirp User's Manual,http://www.nd.edu/~ccl/software/manuals/chirp.html).

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_PAIR(-a,method)Enable this authentication method.
OPTION_PAIR(-A,file)Use this file as the default ACL.
OPTION_ITEM(-b)Run as daemon.
OPTION_PAIR(-B,file)Write PID to file.
OPTION_PAIR(-c,dir)Challenge directory for unix filesystem authentication.
OPTION_ITEM(-C)Do not create a core dump, even due to a crash.
OPTION_PAIR(-d,flag)Enable debugging for this sybsystem
OPTION_PAIR(-e,time)Check for presence of parent at this interval. (default is 300s)
OPTION_ITEM(-E)Exit if parent process dies.
OPTION_PAIR(-F,size)Leave this much space free in the filesystem.
OPTION_PAIR(-G,url)Base url for group lookups. (default: disabled)
OPTION_ITEM(-h)Give help information.
OPTION_PAIR(-I,addr)Listen only on this network interface.
OPTION_PAIR(-M,count)Set the maximum number of clients to accept at once. (default unlimited)
OPTION_PAIR(-n,name)Use this name when reporting to the catalog.
OPTION_PAIR(-o,file)Send debugging output to this file.
OPTION_PAIR(-O,bytes)Rotate debug file once it reaches this size.
OPTION_PAIR(-p,port)Listen on this port (default is 9094)
OPTION_PAIR(-P,user)Superuser for all directories. (default is none)
OPTION_PAIR(-Q,size)Enforce this root quota in software.
OPTION_PAIR(-r,url)URL of storage directory, like file://path or hdfs://host:port/path.
OPTION_ITEM(-R)Read-only mode.
OPTION_PAIR(-s,time)Abort stalled operations after this long. (default is 3600s)
OPTION_PAIR(-t,time)Disconnect idle clients after this time. (default is 60s)
OPTION_PAIR(-T,time)Maximum time to cache group information. (default is 900s)
OPTION_PAIR(-u,host)Send status updates to this host. (default is chirp.cse.nd.edu)
OPTION_PAIR(-U,time)Send status updates at this interval. (default is 5m)
OPTION_ITEM(-v)Show version info.
OPTION_PAIR(-w,name)The name of this server's owner.  (default is username)
OPTION_PAIR(-W,file)Use alternate password file for unix authentication
OPTION_PAIR(-y,dir)Location of transient data (default is pwd).
OPTION_PAIR(-z,time)Set max timeout for unix filesystem authentication. (default is 5s)
OPTION_PAIR(-Z,file)Select port at random and write to this file.  (default is disabled)
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM()CODE(BOLD(CATALOG_HOST)) Hostname of catalog server (same as CODE(-u)).
LIST_ITEM()CODE(BOLD(TCP_LOW_PORT)) Inclusive low port in range used with CODE(-Z).
LIST_ITEM()CODE(BOLD(TCP_HIGH_PORT)) Inclusive high port in range used with CODE(-Z).
LIST_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To start a Chirp server with a local root directory:

LONGCODE_BEGIN
% chirp_server -r file:///tmp/foo
LONGCODE_END

Setting various authentication modes:

LONGCODE_BEGIN
% chirp_server -a hostname -a unix -a ticket -r file:///tmp/foo
LONGCODE_END

SECTION(COPYRIGHT)
COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)
SEE_ALSO_CHIRP

FOOTER

