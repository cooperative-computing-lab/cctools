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
OPTION_TRIPLET(-A, default-acl,file)Use this file as the default ACL.
OPTION_TRIPLET(-a, auth,method)Enable this authentication method.
OPTION_ITEM(`-b, --background')Run as daemon.
OPTION_TRIPLET(-B, pid-file,file)Write PID to file.
OPTION_ITEM(`-C, --no-core-dump')Do not create a core dump, even due to a crash.
OPTION_TRIPLET(-c, chalenge-dir,dir)Challenge directory for unix filesystem authentication.
OPTION_TRIPLET(-d, debug, flag)Enable debugging for this sybsystem
OPTION_ITEM(`-E, --parent-death')Exit if parent process dies.
OPTION_TRIPLET(-e, parent-check,time)Check for presence of parent at this interval. (default is 300s)
OPTION_TRIPLET(-F, free-space,size)Leave this much space free in the filesystem.
OPTION_TRIPLET(-G,group-url, url)Base url for group lookups. (default: disabled)
OPTION_ITEM(`-h, --help')Give help information.
OPTION_TRIPLET(-I, interface,addr)Listen only on this network interface.
OPTION_TRIPLET(-M, max-clients,count)Set the maximum number of clients to accept at once. (default unlimited)
OPTION_TRIPLET(-n, catalog-name,name)Use this name when reporting to the catalog.
OPTION_TRIPLET(-o,debug-file, file)Send debugging output to this file.
OPTION_TRIPLET(-O, debug-rotate-max,bytes)Rotate debug file once it reaches this size.
OPTION_TRIPLET(-P,superuser,user)Superuser for all directories. (default is none)
OPTION_TRIPLET(-p,port,port)Listen on this port (default is 9094)
OPTION_TRIPLET(-Q,root-quota,size)Enforce this root quota in software.
OPTION_ITEM(`-R, --read-only')Read-only mode.
OPTION_TRIPLET(-r, root,url)URL of storage directory, like file://path or hdfs://host:port/path.
OPTION_TRIPLET(-s,stalled,time)Abort stalled operations after this long. (default is 3600s)
OPTION_TRIPLET(-T,group-cache-exp,time)Maximum time to cache group information. (default is 900s)
OPTION_TRIPLET(-t,idle-clients,time)Disconnect idle clients after this time. (default is 60s)
OPTION_TRIPLET(-U,catalog-update,time)Send status updates at this interval. (default is 5m)
OPTION_TRIPLET(-u,advertize,host)Send status updates to this host. (default is catalog.cse.nd.edu)
OPTION_ITEM(`-v, --version')Show version info.
OPTION_TRIPLET(-W,passwd,file)Use alternate password file for unix authentication
OPTION_TRIPLET(-w,owner,name)The name of this server's owner.  (default is username)
OPTION_TRIPLET(-y,transient,dir)Location of transient data (default is pwd).
OPTION_TRIPLET(-Z,port-file,file)Select port at random and write it to this file.  (default is disabled)
OPTION_TRIPLET(-z, unix-timeout,time)Set max timeout for unix filesystem authentication. (default is 5s)
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

