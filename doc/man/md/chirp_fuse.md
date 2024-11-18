






















# chirp_fuse(1)

## NAME
**chirp_fuse** - create a **FUSE** mount point with virtual access to remote chirp servers

## SYNOPSIS
**chirp_fuse [options] _&lt;mount path&gt;_**

## DESCRIPTION


You can attach to a Chirp filesystems by using the FUSE package to attach Chirp
as a kernel filesystem module. Unlike [parrot_run(1)](parrot_run.md), this requires
superuser privileges to install the FUSE package, but will likely work more
reliably on a larger number of programs and environments. Using FUSE allows for
transparent access to a Chirp server via the local filesystem. This allows user
applications to use a Chirp store unmodified.


Once you have installed and permissions to use FUSE, simply run chirp_fuse with
the name of a directory on which the filesystem should be mounted.


For complete details with examples, see the
[Chirp User's Manual](http://ccl.cse.nd.edu/software/manuals/chirp.html).

## OPTIONS


- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-b**,**--block-size=_&lt;bytes&gt;_**<br />Block size for network I/O. (default is 65536s)
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-D**,**--no-optimize**<br />Disable small file optimizations such as recursive delete.
- **-f**,**--foreground**<br />Run in foreground for debugging.
- **-s**,**--single-server=_&lt;hostport&gt;_**<br />Connect only to the named host:port and hide the global namespace.
- **-i**,**--tickets=_&lt;files&gt;_**<br />Comma-delimited list of tickets to use for authentication.
- **-m**,**--mount-option=_&lt;option&gt;_**<br />Pass mount option to FUSE. Can be specified multiple times.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-t**,**--timeout=_&lt;timeout&gt;_**<br />Timeout for network operations. (default is 60s)
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Give help information.


## ENVIRONMENT VARIABLES


- **CHIRP_CLIENT_TICKETS** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To create and use a **FUSE** mount point for access to remote chirp servers:

```
% chirp_fuse /tmp/chirp-fuse &
% cd /tmp/chirp-fuse
% ls
% cd host:port
% cat foo/bar
% exit
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
