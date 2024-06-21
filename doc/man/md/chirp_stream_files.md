






















# chirp_stream_files(1)

## NAME
**chirp_stream_files** - move data to/from chirp servers in parallel

## SYNOPSIS
**chirp_stream_files [options] _&lt;copy|split|join&gt;_ _&lt;localfile&gt;_ { _&lt;hostname[:port]&gt;_ _&lt;remotefile&gt;_**

## DESCRIPTION

**chirp_stream_files** is a tool for moving data from one machine to and from many machines, with the option to split or join the file along the way.  It is useful for constructing scatter-gather types of applications on top of Chirp.

**chirp_stream_files copy** duplicates a single file to multiple hosts.  The _&lt;localfile&gt;_ argument names a file on the local filesystem.  The command will then open a connection to the following list of hosts, and stream the file to all simultaneously.

**chirp_stream_files split** divides an ASCII file up among multiple hosts.  The first line of _&lt;localfile&gt;_ is sent to the first host, the second line to the second, and so on, round-robin.

**chirp_stream_files join** collects multiple remote files into one.  The argument _&lt;localfile&gt;_ is opened for writing, and the remote files for reading.  The remote files are read line-by-line and assembled round-robin into the local file.

In all cases, files are accessed in a streaming manner, making this particularly efficient for processing large files.  A local file name of **-** indicates standard input or standard output, so that the command can be used in a pipeline.
## OPTIONS


- **-a**,**--auth=_&lt;flag&gt;_**<br /> Enable authentication mode: unix, hostname, address, ticket, kerberos, or globus.
- **-b**,**--block-size=_&lt;size&gt;_**<br />Set transfer buffer size. (default is 1048576 bytes)
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem.
- **-i**,**--tickes=_&lt;files&gt;_**<br />Comma-delimited list of tickets to use for authentication.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Timeout for failure. (default is 3600s)
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Show help text.


## ENVIRONMENT VARIABLES
List any environment variables used or set in this section.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To copy the file **mydata** to three locations:

```
% chirp_stream_files copy mydata server1.somewhere.edu /mydata
                                 server2.somewhere.edu /mydata
                                 server2.somewhere.edu /mydata
```

To split the file **mydata** into subsets at three locations:

```
% chirp_stream_files split mydata server1.somewhere.edu /part1
                                  server2.somewhere.edu /part2
                                  server2.somewhere.edu /part3
```

To join three remote files back into one called **newdata**:

```
% chirp_stream_files join newdata server1.somewhere.edu /part1
                                  server2.somewhere.edu /part2
                                  server2.somewhere.edu /part3
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
