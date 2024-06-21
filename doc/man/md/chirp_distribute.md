






















# chirp_stream_files(1)

## NAME
**chirp_distribute** - copy a directory from one Chirp server to one or more Chirp server(s).

## SYNOPSIS
**chirp_distribute [options] _&lt;sourcehost&gt;_ _&lt;sourcepath&gt;_ _&lt;host1&gt;_ _&lt;host2&gt;_ _&lt;host3&gt;_ ...**

## DESCRIPTION
**chirp_distribute** is a tool for coping commonly used data across Chirp servers. Data is originated from a Chirp server _&lt;sourcehost&gt;_ _&lt;sourcepath&gt;_ and is copied to a given list of Chirp server(s) _&lt;host1&gt;_ _&lt;host2&gt;_ _&lt;host3&gt;_, etc ...

**chirp_distribute** is a quick and simple way for replicating a directory from a Chirp server to many Chirp Servers by creating a spanning tree and then transferring data concurrently from host to host using third party transfer. It is faster than manually copying data using **parrot cp**, **chirp_put** or **chirp_third_put**

**chirp_distribute** also can clean up replicated data using -X option.
## OPTIONS


- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-D**,**--info-transfer**<br />Show detailed location, time, and performance of each transfer.
- **-F**,**--failures-file=_&lt;file&gt;_**<br />Write matrix of failures to this file.
- **-i**,**--tickets=_&lt;files&gt;_**<br />Comma-delimited list of tickets to use for authentication.
- **-N**,**--copies-max=_&lt;num&gt;_**<br />Stop after this number of successful copies.
- **-p**,**--jobs=_&lt;num&gt;_**<br />Maximum number of processes to run at once (default=100)
- **-R**,**--randomize-hosts**<br />Randomize order of target hosts given on command line.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Timeout for for each copy. (default is 3600s)
- **-T**,**--timeout-all=_&lt;time&gt;_**<br />Overall timeout for entire distribution. (default is 3600).
- **-v**,**--verbose**<br />Show program version.
- **-X**,**--delete-target**<br />Delete data from all of the target hosts.
- **-Y**,**--info-success**<br />Show confirmation of successful placements.
- **-h**,**--help**<br />Show help text.


## ENVIRONMENT VARIABLES


- **CHIRP_CLIENT_TICKETS** Comma delimited list of tickets to authenticate with (same as **-i**).


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

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
