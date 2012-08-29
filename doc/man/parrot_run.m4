include(manual.h)dnl
HEADER(parrot_run)dnl

SECTION(NAME)
BOLD(parrot_run) - run a program in the Parrot virtual file system

SECTION(SYNOPSIS)
CODE(BOLD(parrot_run [parrot_options] program [program_options]))

SECTION(DESCRIPTION)
CODE(parrot_run) runs an application or a shell inside the Parrot virtual filesystem.  Parrot redirects the application's system calls to remote storage systems.  Parrot currently supports the following remote storage systems: HTTP, GROW, FTP, GridFTP, iRODS, HDFS, XRootd, Chirp.  This list may vary depending on how Parrot was built.  Run CODE(parrot -h) to see exactly what support is available on your system.
PARA
Parrot works by trapping the application's system calls through the CODE(ptrace) debugging interface.  It does not require any special privileges to install or run, so it is useful to ordinary users that wish to access data across wide area networks.  The CODE(ptrace) debugging interface does have some cost, so applications may run slower, depending on how many I/O operations they perform.
PARA
For complete details with examples, see the LINK(Parrot User's Manual,http://www.nd.edu/~ccl/software/manuals/parrot.html)

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_PAIR(-a, unix|hostname|ticket|globus|kerberos)Use this Chirp authentication method.  May be invoked multiple times to indicate a preferred list, in order.
OPTION_PAIR(-A, file)Use this file as a default ACL.
OPTION_PAIR(-b, bytes)Set the I/O block size hint.
OPTION_PAIR(-c, file)Print exit status information to file.
OPTION_ITEM(-C)Enable data channel authentication in GridFTP.
OPTION_PAIR(-d, name)Enable debugging for this sub-system.
OPTION_ITEM(-D)Disable small file optimizations.
OPTION_ITEM(-F)Enable file snapshot caching for all protocols.
OPTION_ITEM(-f)Disable following symlinks.
OPTION_PAIR(-E, url)Endpoint for gLite combined catalog ifc.
OPTION_PAIR(-G, gid)Fake this gid; Real gid stays the same.
OPTION_ITEM(-H)Disable use of helper library.
OPTION_ITEM(-h)Show this screen.
OPTION_ITEM(-I) Set the iRODS driver internal debug level.
OPTION_ITEM(-K)Checksum files where available.
OPTION_ITEM(-k)Do not checksum files.
OPTION_PAIR(-l, path)Path to ld.so to use.
OPTION_PAIR(-m, file)Use this file as a mountlist.
OPTION_PAIR(-M, /foo=/bar)Mount (redirect) /foo to /bar.
OPTION_PAIR(-N, name)Pretend that this is my hostname.
OPTION_PAIR(-o, file)Send debugging messages to this file.
OPTION_PAIR(-O, bytes)Rotate debug files of this size.
OPTION_PAIR(-p, host:port)Use this proxy server for HTTP requests.
OPTION_ITEM(-Q)Inhibit catalog queries to list /chirp.
OPTION_PAIR(-r, repos)CVMFS repositories to enable (PARROT_CVMFS_REPO).
OPTION_PAIR(-R, cksum)Enforce this root filesystem checksum, where available.
OPTION_ITEM(-s)Use streaming protocols without caching.
OPTION_ITEM(-S)Enable whole session caching for all protocols.
OPTION_PAIR(-t, dir)Where to store temporary files.
OPTION_PAIR(-T, time)Maximum amount of time to retry failures.
OPTION_PAIR(-U, uid)Fake this unix uid; Real uid stays the same.
OPTION_PAIR(-u, name)Use this extended username.
OPTION_ITEM(-v)Display version number.
OPTION_ITEM(-w)Initial working directory.
OPTION_ITEM(-W)Display table of system calls trapped.
OPTION_ITEM(-Y)Force sYnchronous disk writes.
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)
CODE(parrot_run) sets the environment variable CODE(PARROT_ENABLED) to the value CODE(1)
for its child processes.  This makes it possible to set a visible flag in your shell prompt
when CODE(parrot_run) is enabled.

SECTION(EXIT STATUS)
CODE(parrot_run) returns the exit status of the process that it runs.
If CODE(parrot_run) is unable to start the process, it will return non-zero.

SECTION(EXAMPLES)
To access a single remote file using CODE(vi):
LONGCODE_BEGIN
% parrot_run vi /anonftp/ftp.gnu.org/pub/README
LONGCODE_END

You can also run an entire shell inside of Parrot, like this:
LONGCODE_BEGIN
% parrot_run bash
% cd /anonftp/ftp.gnu.org/pub
% ls -la
% cat README
% exit
LONGCODE_END

To see the list of available Chirp servers around the world:
LONGCODE_BEGIN
% parrot_run ls -la /chirp
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER

