include(manual.h)dnl
HEADER(parrot_run)

SECTION(NAME)
BOLD(parrot_run) - run a program in the Parrot virtual file system

SECTION(SYNOPSIS)
CODE(parrot_run [parrot_options] program [program_options])

SECTION(DESCRIPTION)
CODE(parrot_run) runs an application or a shell inside the Parrot virtual filesystem.  Parrot redirects the application's system calls to remote storage systems.  Parrot currently supports the following remote storage systems: HTTP, GROW, FTP, GridFTP, iRODS, HDFS, XRootd, Chirp.  This list may vary depending on how Parrot was built.  Run CODE(parrot -h) to see exactly what support is available on your system.
PARA
Parrot works by trapping the application's system calls through the CODE(ptrace) debugging interface.  It does not require any special privileges to install or run, so it is useful to ordinary users that wish to access data across wide area networks.  The CODE(ptrace) debugging interface does have some cost, so applications may run slower, depending on how many I/O operations they perform.
PARA
For complete details with examples, see the LINK(Parrot User's Manual,http://ccl.cse.nd.edu/software/manuals/parrot.html)

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_ARG_LONG(check-driver,driver) Check for the presence of a given driver (e.g. http, ftp, etc) and return success if it is currently enabled.
OPTION_ARG(a,chirp-auth,unix|hostname|ticket|globus|kerberos)Use this Chirp authentication method.  May be invoked multiple times to indicate a preferred list, in order.
OPTION_ARG(b,block-size,bytes)Set the I/O block size hint.
OPTION_ARG(c,status-file,file)Print exit status information to file.
OPTION_FLAG(C,channel-auth)Enable data channel authentication in GridFTP.
OPTION_ARG(d,debug,flag)Enable debugging for this sub-system.
OPTION_FLAG(D,no-optimize)Disable small file optimizations.
OPTION_FLAG_LONG(dynamic-mounts) Enable the use of parot_mount in this session.
OPTION_FLAG(F,with-snapshots)Enable file snapshot caching for all protocols.
OPTION_FLAG(f,no-follow-symlinks)Disable following symlinks.
OPTION_ARG(G,gid,num)Fake this gid; Real gid stays the same.
OPTION_FLAG(h,help)Show this screen.
OPTION_FLAG_LONG(--helper)Enable use of helper library.
OPTION_ARG(i,tickets,files)Comma-delimited list of tickets to use for authentication.
OPTION_ARG(I,debug-level-irods,num)Set the iRODS driver internal debug level.
OPTION_FLAG(K,with-checksums)Checksum files where available.
OPTION_FLAG(k,no-checksums)Do not checksum files.
OPTION_ARG(l,ld-path,path)Path to ld.so to use.
OPTION_ARG(m,ftab-file,file)Use this file as a mountlist.
OPTION_ARG(M,mount,/foo=/bar)Mount (redirect) /foo to /bar.
OPTION_ARG(e,env-list,path)Record the environment variables.
OPTION_ARG(n,name-list,path)Record all the file names.
OPTION_FLAG_LONG(no-set-foreground)Disable changing the foreground process group of the session.
OPTION_ARG(N,hostname,name)Pretend that this is my hostname.
OPTION_ARG(o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
OPTION_ARG(O,debug-rotate-max, bytes)Rotate debug files of this size.
OPTION_ARG(p,proxy,host:port)Use this proxy server for HTTP requests.
OPTION_FLAG(Q,no-chirp-catalog)Inhibit catalog queries to list /chirp.
OPTION_ARG(r,cvmfs-repos,repos)CVMFS repositories to enable (PARROT_CVMFS_REPO).
OPTION_FLAG_LONG(cvmfs-repo-switching) Allow repository switching with CVMFS.
OPTION_ARG(R,root-checksum,cksum)Enforce this root filesystem checksum, where available.
OPTION_FLAG(s,stream-no-cache)Use streaming protocols without caching.
OPTION_FLAG(S,session-caching)Enable whole session caching for all protocols.
OPTION_FLAG_LONG(syscall-disable-debug)Disable tracee access to the Parrot debug syscall.
OPTION_ARG(t,tempdir,dir)Where to store temporary files.
OPTION_ARG(T,timeout,time)Maximum amount of time to retry failures.
time)Maximum amount of time to retry failures.
OPTION_FLAG_LONG(time-stop) Stop virtual time at midnight, Jan 1st, 2001 UTC.
OPTION_FLAG_LONG(time-warp) Warp virtual time starting from midnight, Jan 1st, 2001 UTC.
OPTION_ARG(U,uid,num)Fake this unix uid; Real uid stays the same.
OPTION_ARG(u,username, name)Use this extended username.
OPTION_FLAG_LONG(fake-setuid)Track changes from setuid and setgid.
OPTION_FLAG_LONG(valgrind)Enable valgrind support for Parrot.
OPTION_FLAG(v,version)Display version number.
OPTION_FLAG_LONG(is-running)Test is Parrot is already running.
OPTION_ARG(w,work-dir, dir)Initial working directory.
OPTION_FLAG(W,syscall-table)Display table of system calls trapped.
OPTION_FLAG(Y,sync-write)Force synchronous disk writes.
OPTION_FLAG(Z,auto-decompress)Enable automatic decompression on .gz files.
OPTION_ARG_LONG(disable-service,service) Disable a compiled-in service (e.g. http, cvmfs, etc.)
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

Parrot can record the names of all the accessed files and the environment variables during the execution process of one program, like this:
LONGCODE_BEGIN
% parrot_run --name-list list.txt --env-list envlist ls ~
LONGCODE_END
The environment variables at the starting moment of your program will be recorded into BOLD(envlist). The absolute paths of all the accessed files, together with the system call types, will be recorded into BOLD(list.txt). For example, the file BOLD(/usr/bin/ls) is accessed using the BOLD(stat) system call, like this:
LONGCODE_BEGIN
% /usr/bin/ls|stat
LONGCODE_END

SECTION(NOTES ON DOCKER)

Docker by default blocks ptrace, the system call on which parrot relies. To
run parrot inside docker, the container needs to be started using the
CODE(--security-opt seccomp=unconfined) command line argument. For
example:

LONGCODE_BEGIN
    docker run --security-opt seccomp=unconfined MY-DOCKER-IMAGE
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_PARROT

FOOTER
