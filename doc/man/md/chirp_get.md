






















# chirp_get(1)

## NAME
**chirp_get** - get a single file from a Chirp server to local machine

## SYNOPSIS
****chirp_get [options] <hostname[:port]> <remotefile> <localfile>****

## DESCRIPTION

**chirp_get** is a tool for copying a single file from a Chirp server to local storage.

**chirp_get** is a quick and simple way to copy a remote file given <hostname[:port]> <path> and write it to a local file <localfile>

**chirp_get** also can stream data which can be useful in a shell pipeline.

## OPTIONS


- **-a --auth <flag>** Require this authentication mode.
- **-d --debug <flag>** Enable debugging for this subsystem.
- **-t --timeout <time>** Timeout for failure. (default is 3600s)
- **-i --tickets <files>** Comma-delimited list of tickets to use for authentication.
- **-v, --version** Show program version.
- **-h, --help** Show help text.


## ENVIRONMENT VARIABLES


- ****CHIRP_CLIENT_TICKETS**** Comma delimited list of tickets to authenticate with (same as **-i**).


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

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools 8.0.0 DEVELOPMENT
