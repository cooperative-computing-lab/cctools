






















# chirp_server(1)

## NAME
**chirp_server** - create a Chirp user-level filesystem

## SYNOPSIS
**chirp_server [options]**

## DESCRIPTION


Starts a Chirp server which allows the sharing of data with friends and
colleagues without requiring any administrator privileges.  Chirp provides an
RPC network interface to a "backend" filesystem which can be the local
filesystem or even the Hadoop HDFS filesystem. Chirp supports flexible and
robust ACL management of data.


For complete details with examples, see the [Chirp User's Manual](http://ccl.cse.nd.edu/software/manuals/chirp.html).

## OPTIONS


- **-A**,**--default-acl=_&lt;file&gt;_**<br />Use this file as the default ACL.
- **--inherit-default-acl**<br /> Directories without an ACL inherit from parent directories.
- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-b**,**--background**<br />Run as daemon.
- **-B**,**--pid-file=_&lt;file&gt;_**<br />Write PID to file.
- **-C**,**--no-core-dump**<br />Do not create a core dump, even due to a crash.
- **-c**,**--challenge-dir=_&lt;dir&gt;_**<br />Challenge directory for unix filesystem authentication.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this sybsystem
- **-E**,**--parent-death**<br />Exit if parent process dies.
- **-e**,**--parent-check=_&lt;time&gt;_**<br />Check for presence of parent at this interval. (default is 300s)
- **-F**,**--free-space=_&lt;size&gt;_**<br />Leave this much space free in the filesystem.
- **-G**,**--group-url=_&lt;url&gt;_**<br />Base url for group lookups. (default: disabled)
- **-h**,**--help**<br />Give help information.
- **-I**,**--interface=_&lt;addr&gt;_**<br />Listen only on this network interface.
- **-M**,**--max-clients=_&lt;count&gt;_**<br />Set the maximum number of clients to accept at once. (default unlimited)
- **--max-ticket-duration=_&lt;time&gt;_**<br />Set max duration for authentication tickets, in seconds. (default is unlimited)
- **-n**,**--catalog-name=_&lt;name&gt;_**<br />Use this name when reporting to the catalog.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-O**,**--debug-rotate-max=_&lt;bytes&gt;_**<br />Rotate debug file once it reaches this size.
- **-P**,**--superuser=_&lt;user&gt;_**<br />Superuser for all directories. (default is none)
- **-p**,**--port=_&lt;port&gt;_**<br />Listen on this port (default is 9094, arbitrary is 0)
- **--project-name=_&lt;name&gt;_**<br />Project name this Chirp server belongs to.
- **-Q**,**--root-quota=_&lt;size&gt;_**<br />Enforce this root quota in software.
- **-R**,**--read-only**<br />Read-only mode.
- **-r**,**--root=_&lt;url&gt;_**<br />URL of storage directory, like file://path or hdfs://host:port/path.
- **-s**,**--stalled=_&lt;time&gt;_**<br />Abort stalled operations after this long. (default is 3600s)
- **-T**,**--group-cache-exp=_&lt;time&gt;_**<br />Maximum time to cache group information. (default is 900s)
- **-t**,**--idle-clients=_&lt;time&gt;_**<br />Disconnect idle clients after this time. (default is 60s)
- **-U**,**--catalog-update=_&lt;time&gt;_**<br />Send status updates at this interval. (default is 5m)
- **-u**,**--advertize=_&lt;host&gt;_**<br />Send status updates to this host. (default is catalog.cse.nd.edu)
- **-v**,**--version**<br />Show version info.
- **-W**,**--passwd=_&lt;file&gt;_**<br />Use alternate password file for unix authentication
- **-w**,**--owner=_&lt;name&gt;_**<br />The name of this server's owner.  (default is username)
- **-y**,**--transient=_&lt;dir&gt;_**<br />Location of transient data (default is pwd).
- **-Z**,**--port-file=_&lt;file&gt;_**<br />Select port at random and write it to this file.  (default is disabled)
- **-z**,**--unix-timeout=_&lt;time&gt;_**<br />Set max timeout for unix filesystem authentication. (default is 5s)


## ENVIRONMENT VARIABLES


- **CATALOG_HOST** Hostname of catalog server (same as **-u**).
- **TCP_LOW_PORT** Inclusive low port in range used with **-Z**.
- **TCP_HIGH_PORT** Inclusive high port in range used with **-Z**.


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
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
