






















# parrot_run(1)

## NAME
**parrot_run** - run a program in the Parrot virtual file system

## SYNOPSIS
****parrot_run [parrot_options] program [program_options]****

## DESCRIPTION
**parrot_run** runs an application or a shell inside the Parrot virtual filesystem.  Parrot redirects the application's system calls to remote storage systems.  Parrot currently supports the following remote storage systems: HTTP, GROW, FTP, GridFTP, iRODS, HDFS, XRootd, Chirp.  This list may vary depending on how Parrot was built.  Run **parrot -h** to see exactly what support is available on your system.

Parrot works by trapping the application's system calls through the **ptrace** debugging interface.  It does not require any special privileges to install or run, so it is useful to ordinary users that wish to access data across wide area networks.  The **ptrace** debugging interface does have some cost, so applications may run slower, depending on how many I/O operations they perform.

For complete details with examples, see the [Parrot User's Manual](http://ccl.cse.nd.edu/software/manuals/parrot.html)

## OPTIONS

- **--check-driver driver**  Check for the presence of a given driver (e.g. http, ftp, etc) and return success if it is currently enabled.
- **-a --chirp-auth <unix|hostname|ticket|globus|kerberos>** Use this Chirp authentication method.  May be invoked multiple times to indicate a preferred list, in order.
- **-b --block-size <bytes>** Set the I/O block size hint.
- **-c --status-file <file>** Print exit status information to file.
- **-C** Enable data channel authentication in GridFTP.
- **-d --debug <flag>** Enable debugging for this sub-system.
- **-D** Disable small file optimizations.
- **--dynamic-mounts**  Enable the use of parot_mount in this session.
- **-F** Enable file snapshot caching for all protocols.
- **-f** Disable following symlinks.
- **-G --gid <num>** Fake this gid; Real gid stays the same.
- **-h** Show this screen.
- **--helper** Enable use of helper library.
- **-i --tickets <files>** Comma-delimited list of tickets to use for authentication.
- **-I --debug-level-irods <num>** Set the iRODS driver internal debug level.
- **-K** Checksum files where available.
- **-k** Do not checksum files.
- **-l --ld-path <path>** Path to ld.so to use.
- **-m --ftab-file <file>** Use this file as a mountlist.
- **-M --mount </foo=/bar>** Mount (redirect) /foo to /bar.
- **-e --env-list <path>** Record the environment variables.
- **-n --name-list <path>** Record all the file names.
- **--no-set-foreground** Disable changing the foreground process group of the session.
- **-N --hostname <name>** Pretend that this is my hostname.
- **-o --debug-file <file>** Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-O --debug-rotate-max <bytes>** Rotate debug files of this size.
- **-p --proxy <host:port>** Use this proxy server for HTTP requests.
- **-Q** Inhibit catalog queries to list /chirp.
- **-r --cvmfs-repos <repos>** CVMFS repositories to enable (PARROT_CVMFS_REPO).
- **--cvmfs-repo-switching**  Allow repository switching with CVMFS.
- **-R --root-checksum <cksum>** Enforce this root filesystem checksum, where available.
- **-s** Use streaming protocols without caching.
- **-S** Enable whole session caching for all protocols.
- **--syscall-disable-debug** Disable tracee access to the Parrot debug syscall.
- **-t --tempdir <dir>** Where to store temporary files.
- **-T --timeout <time>** Maximum amount of time to retry failures.
time)Maximum amount of time to retry failures.
- **--time-stop**  Stop virtual time at midnight, Jan 1st, 2001 UTC.
- **--time-warp**  Warp virtual time starting from midnight, Jan 1st, 2001 UTC.
- **-U --uid <num>** Fake this unix uid; Real uid stays the same.
- **-u --username <name>** Use this extended username.
- **--fake-setuid** Track changes from setuid and setgid.
- **--valgrind** Enable valgrind support for Parrot.
- **-v** Display version number.
- **--is-running** Test is Parrot is already running.
- **-w --work-dir <dir>** Initial working directory.
- **-W** Display table of system calls trapped.
- **-Y** Force synchronous disk writes.
- **-Z** Enable automatic decompression on .gz files.
- **--disable-service service**  Disable a compiled-in service (e.g. http, cvmfs, etc.)


## ENVIRONMENT VARIABLES
**parrot_run** sets the environment variable **PARROT_ENABLED** to the value **1**
for its child processes.  This makes it possible to set a visible flag in your shell prompt
when **parrot_run** is enabled.

## EXIT STATUS
**parrot_run** returns the exit status of the process that it runs.
If **parrot_run** is unable to start the process, it will return non-zero.

## EXAMPLES
To access a single remote file using **vi**:
```
% parrot_run vi /anonftp/ftp.gnu.org/pub/README
```

You can also run an entire shell inside of Parrot, like this:
```
% parrot_run bash
% cd /anonftp/ftp.gnu.org/pub
% ls -la
% cat README
% exit
```

To see the list of available Chirp servers around the world:
```
% parrot_run ls -la /chirp
```

Parrot can record the names of all the accessed files and the environment variables during the execution process of one program, like this:
```
% parrot_run --name-list list.txt --env-list envlist ls ~
```
The environment variables at the starting moment of your program will be recorded into **envlist**. The absolute paths of all the accessed files, together with the system call types, will be recorded into **list.txt**. For example, the file **/usr/bin/ls** is accessed using the **stat** system call, like this:
```
% /usr/bin/ls|stat
```

## NOTES ON DOCKER

Docker by default blocks ptrace, the system call on which parrot relies. To
run parrot inside docker, the container needs to be started using the
**--security-opt seccomp=unconfined** command line argument. For
example:

```
    docker run --security-opt seccomp=unconfined MY-DOCKER-IMAGE
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Parrot User Manual]("../parrot.html")
- [parrot_run(1)](parrot_run.md) [parrot_cp(1)](parrot_cp.md) [parrot_getacl(1)](parrot_getacl.md)  [parrot_setacl(1)](parrot_setacl.md)  [parrot_mkalloc(1)](parrot_mkalloc.md)  [parrot_lsalloc(1)](parrot_lsalloc.md)  [parrot_locate(1)](parrot_locate.md)  [parrot_timeout(1)](parrot_timeout.md)  [parrot_whoami(1)](parrot_whoami.md)  [parrot_mount(1)](parrot_mount.md)  [parrot_md5(1)](parrot_md5.md)  [parrot_package_create(1)](parrot_package_create.md)  [parrot_package_run(1)](parrot_package_run.md)  [chroot_package_run(1)](chroot_package_run.md)


CCTools 8.0.0 DEVELOPMENT released on 
