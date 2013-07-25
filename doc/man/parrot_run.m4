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
OPTION_TRIPLET(-a,chirp-auth,unix|hostname|ticket|globus|kerberos)Use this Chirp authentication method.  May be invoked multiple times to indicate a preferred list, in order.
OPTION_TRIPLET(-b, block-size, bytes)Set the I/O block size hint.
OPTION_TRIPLET(-c, status-file, file)Print exit status information to file.
OPTION_ITEM(`-C, channel-auth')Enable data channel authentication in GridFTP.
OPTION_TRIPLET(-d, debug, flag)Enable debugging for this sub-system.
OPTION_ITEM(`-D, --no-optimize')Disable small file optimizations.
OPTION_ITEM(`-F, --with-snapshots')Enable file snapshot caching for all protocols.
OPTION_ITEM(`-f, --no-follow-symlinks')Disable following symlinks.
OPTION_TRIPLET(-G,gid,num)Fake this gid; Real gid stays the same.
OPTION_ITEM(`-H, --no-helper')Disable use of helper library.
OPTION_ITEM(`-h, --help')Show this screen.
OPTION_TRIPLET(-i, tickets, files)Comma-delimited list of tickets to use for authentication.
OPTION_TRIPLET(-I, debug-level-irods, num)Set the iRODS driver internal debug level.
OPTION_ITEM(`-K, --with-checksums')Checksum files where available.
OPTION_ITEM(`-k, --no-checksums')Do not checksum files.
OPTION_TRIPLET(-l, ld-path, path)Path to ld.so to use.
OPTION_TRIPLET(-m, ftab-file, file)Use this file as a mountlist.
OPTION_TRIPLET(-M, mount, /foo=/bar)Mount (redirect) /foo to /bar.
OPTION_TRIPLET(-N, hostname, name)Pretend that this is my hostname.
OPTION_TRIPLET(-o, debug-file, file)Send debugging messages to this file.
OPTION_TRIPLET(-O, debug-rotate-max, bytes)Rotate debug files of this size.
OPTION_TRIPLET(-p, proxy, host:port)Use this proxy server for HTTP requests.
OPTION_ITEM(`-Q, --no-chirp-catalog')Inhibit catalog queries to list /chirp.
OPTION_TRIPLET(-r, cvmfs-repos, repos)CVMFS repositories to enable (PARROT_CVMFS_REPO).
OPTION_ITEM(--cvmfs-repo-switching) Allow repository switching with CVMFS.
OPTION_TRIPLET(-R, root-checksum, cksum)Enforce this root filesystem checksum, where available.
OPTION_ITEM(`-s, --stream-no-cache')Use streaming protocols without caching.
OPTION_ITEM(`-S, --session-caching')Enable whole session caching for all protocols.
OPTION_TRIPLET(-t, tempdir, dir)Where to store temporary files.
OPTION_TRIPLET(-T, timeout, time)Maximum amount of time to retry failures.
OPTION_TRIPLET(-U, uid, num)Fake this unix uid; Real uid stays the same.
OPTION_TRIPLET(-u, username, name)Use this extended username.
OPTION_ITEM(`-v, --version')Display version number.
OPTION_TRIPLET(-w, work-dir, dir)Initial working directory.
OPTION_ITEM(`-W, --syscall-table')Display table of system calls trapped.
OPTION_ITEM(`-Y, --sync-write')Force synchronous disk writes.
OPTION_ITEM(`-Z, --auto-decompress')Enable automatic decompression on .gz files.
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

