






















# chirp_get(1)

## NAME
**chirp_get** - get a single file from a Chirp server to local machine

## SYNOPSIS
**chirp_get [options] _&lt;hostname[:port]&gt;_ _&lt;remotefile&gt;_ _&lt;localfile&gt;_**

## DESCRIPTION

**chirp_get** is a tool for copying a single file from a Chirp server to local storage.

**chirp_get** is a quick and simple way to copy a remote file given _&lt;hostname[:port]&gt;_ _&lt;path&gt;_ and write it to a local file _&lt;localfile&gt;_

**chirp_get** also can stream data which can be useful in a shell pipeline.

## OPTIONS


- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Timeout for failure. (default is 3600s)
- **-i**,**--tickets=_&lt;files&gt;_**<br />Comma-delimited list of tickets to use for authentication.
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Show help text.


## ENVIRONMENT VARIABLES


- **CHIRP_CLIENT_TICKETS** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To copy a single remote file using **chirp_get**:

```
% chirp_get server1.somewhere.edu /mydata/mydata.dat /tmp/mydata.dat
```

To get, while at the same time, untar a single remote archive file using **chirp_get**:

```
% chirp_get myhost.somewhere.edu /mydata/archive.tar.gz - | tar xvzf
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
