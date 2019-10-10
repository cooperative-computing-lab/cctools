






















# chirp_server(1)

## NAME
**chirp_server** - create a Chirp user-level filesystem

## SYNOPSIS
****chirp_server [options]****

## DESCRIPTION


Starts a Chirp server which allows the sharing of data with friends and
colleagues without requiring any administrator privileges.  Chirp provides an
RPC network interface to a "backend" filesystem which can be the local
filesystem or even the Hadoop HDFS filesystem. Chirp supports flexible and
robust ACL management of data.


For complete details with examples, see the [Chirp User's Manual](http://ccl.cse.nd.edu/software/manuals/chirp.html).

## OPTIONS


- **-A --default-acl <file>** Use this file as the default ACL.
- **--inherit-default-acl**  Directories without an ACL inherit from parent directories.
- **-a --auth <method>** Enable this authentication method.
- **-b, --background** Run as daemon.
- **-B --pid-file <file>** Write PID to file.
- **-C, --no-core-dump** Do not create a core dump, even due to a crash.
- **-c --challenge-dir <dir>** Challenge directory for unix filesystem authentication.
- **-d --debug <flag>** Enable debugging for this sybsystem
- **-E, --parent-death** Exit if parent process dies.
- **-e --parent-check <time>** Check for presence of parent at this interval. (default is 300s)
- **-F --free-space <size>** Leave this much space free in the filesystem.
- **-G --group-url <url>** Base url for group lookups. (default: disabled)
- **-h, --help** Give help information.
- **-I --interface <addr>** Listen only on this network interface.
- **-M --max-clients <count>** Set the maximum number of clients to accept at once. (default unlimited)
- **-n --catalog-name <name>** Use this name when reporting to the catalog.
- **-o --debug-file <file>** Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
- **-O --debug-rotate-max <bytes>** Rotate debug file once it reaches this size.
- **-P --superuser <user>** Superuser for all directories. (default is none)
- **-p --port <port>** Listen on this port (default is 9094, arbitrary is 0)
- **--project-name name** Project name this Chirp server belongs to.
- **-Q --root-quota <size>** Enforce this root quota in software.
- **-R, --read-only** Read-only mode.
- **-r --root <url>** URL of storage directory, like file://path or hdfs://host:port/path.
- **-s --stalled <time>** Abort stalled operations after this long. (default is 3600s)
- **-T --group-cache-exp <time>** Maximum time to cache group information. (default is 900s)
- **-t --idle-clients <time>** Disconnect idle clients after this time. (default is 60s)
- **-U --catalog-update <time>** Send status updates at this interval. (default is 5m)
- **-u --advertize <host>** Send status updates to this host. (default is catalog.cse.nd.edu)
- **-v, --version** Show version info.
- **-W --passwd <file>** Use alternate password file for unix authentication
- **-w --owner <name>** The name of this server's owner.  (default is username)
- **-y --transient <dir>** Location of transient data (default is pwd).
- **-Z --port-file <file>** Select port at random and write it to this file.  (default is disabled)
- **-z --unix-timeout <time>** Set max timeout for unix filesystem authentication. (default is 5s)


## ENVIRONMENT VARIABLES


- ****CATALOG_HOST**** Hostname of catalog server (same as **-u**).
- ****TCP_LOW_PORT**** Inclusive low port in range used with **-Z**.
- ****TCP_HIGH_PORT**** Inclusive high port in range used with **-Z**.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To start a Chirp server with a local root directory:

```
% chirp_server -r file:///tmp/foo
```

Setting various authentication modes:

```
% chirp_server -a hostname -a unix -a ticket -r file:///tmp/foo
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools 8.0.0 DEVELOPMENT released on 
