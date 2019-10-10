






















# chirp_put(1)

## NAME
**chirp_put** - copy a single file from local machine to a Chirp server

## SYNOPSIS
****chirp_put [options] <localfile> <hostname[:port]> <remotefile>****

## DESCRIPTION

**chirp_put** is a tool for copying a single file from local storage to a Chirp server.

**chirp_put** is a quick and simple way to copy a single local file <localfile> to a remote file given <hostname[:port]> <path>

## OPTIONS


- **-a --auth <flag>** Require this authentication mode.
- **-d --debug <flag>** Enable debugging for this subsystem.
- **-b --block-size <size>** Set transfer buffer size. (default is 65536 bytes).
- **-f, --follow** Follow input file like tail -f.
- **-i --tickets <files>** Comma-delimited list of tickets to use for authentication.
- **-t --timeout <time>** Timeout for failure. (default is 3600s)
- **-v, --version** Show program version.
- **-h, --help** Show help text.


## ENVIRONMENT VARIABLES


- ****CHIRP_CLIENT_TICKETS**** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To copy a single local file using **chirp_put**:

```
% chirp_put /tmp/mydata.dat server1.somewhere.edu /mydata/mydata.dat
```

When copying big data files that take longer than 3600s to copy, using **chirp_put** with option -t time to make sure **chirp_put** have enough time to finish:

```
% chirp_put -t 36000 /tmp/mybigdata.dat server1.somewhere.edu /mydata/mybigdata.dat
```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools 8.0.0 DEVELOPMENT released on 
