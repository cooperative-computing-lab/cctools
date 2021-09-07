






















# chirp_stream_files(1)

## NAME
**chirp_distribute** - copy a directory from one Chirp server to one or more Chirp server(s).

## SYNOPSIS
****chirp_distribute [options] <sourcehost> <sourcepath> <host1> <host2> <host3> ...****

## DESCRIPTION
**chirp_distribute** is a tool for coping commonly used data across Chirp servers. Data is originated from a Chirp server <sourcehost> <sourcepath> and is copied to a given list of Chirp server(s) <host1> <host2> <host3>, etc ...

**chirp_distribute** is a quick and simple way for replicating a directory from a Chirp server to many Chirp Servers by creating a spanning tree and then transferring data concurrently from host to host using third party transfer. It is faster than manually copying data using **parrot cp**, **chirp_put** or **chirp_third_put**

**chirp_distribute** also can clean up replicated data using -X option.
## OPTIONS


- **-a --auth <flag>** Require this authentication mode.
- **-d --debug <flag>** Enable debugging for this subsystem.
- **-D, --info-transfer** Show detailed location, time, and performance of each transfer.
- **-F --failures-file <file>** Write matrix of failures to this file.
- **-i --tickets <files>** Comma-delimited list of tickets to use for authentication.
- **-N --copies-max <num>** Stop after this number of successful copies.
- **-p --jobs <num>** Maximum number of processes to run at once (default=100)
- **-R, --randomize-hosts** Randomize order of target hosts given on command line.
- **-t --timeout <time>** Timeout for for each copy. (default is 3600s)
- **-T --timeout-all <time>** Overall timeout for entire distribution. (default is 3600).
- **-v, --verbose** Show program version.
- **-X, --delete-target** Delete data from all of the target hosts.
- **-Y, --info-success** Show confirmation of successful placements.
- **-h, --help** Show help text.


## ENVIRONMENT VARIABLES


- ****CHIRP_CLIENT_TICKETS**** Comma delimited list of tickets to authenticate with (same as **-i**).


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES
To copy a directory from server1 to server2, server3, server4 using **chirp_distribute**:
```
chirp_distribute server1.somewhere.edu /mydata server2.somewhere.edu server3.somewhere.edu server4.somewhere.edu
```

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using **chirp_distribute** and **chirp_status**:

```

chirp_distribute server1.somewhere.edu /mydata \`chirp_status -s\`

```

To replicate a directory from server1 to  all available Chirp server(s) in Chirp catalog using **chirp_distribute** and **chirp_status**. However stop when reach 100 copies with -N option:
```

chirp_distribute -N 100 server1.somewhere.edu /mydata \`chirp_status -s\`

```

To clean up replicated data using **chirp_distribute** using -X option:
```

chirp_distribute -X server1.somewhere.edu /mydata \`chirp_status -s\`

```


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools 8.0.0 DEVELOPMENT
