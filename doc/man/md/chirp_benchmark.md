






















# chirp_benchmark(1)

## NAME
**chirp_benchmark** - do micro-performance tests on a Chirp server

## SYNOPSIS
**chirp_benchmark _&lt;host[:port]&gt;_ _&lt;file&gt;_ _&lt;loops&gt;_ _&lt;cycles&gt;_ _&lt;bwloops&gt;_**

## DESCRIPTION


**chirp_benchmark** tests a Chirp server's bandwidth and latency for various
Remote Procedure Calls. The command uses a combination of RPCs that do and do
not use I/O bandwidth on the backend filesystem to measure the latency. It also
tests the throughput for reading and writing to the given filename with
various block sizes.


For complete details with examples, see the [Chirp User's Manual](http://ccl.cse.nd.edu/software/manuals/chirp.html).

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To test a Chirp server, you may use:

```
$ chirp_benchmark host:port foobar 10 10 10
getpid     1.0000 +/-    0.1414  usec
write1   496.3200 +/-   41.1547  usec
write8   640.0400 +/-   23.8790  usec
read1     41.0400 +/-   91.3210  usec
read8      0.9200 +/-    0.1789  usec
stat     530.2400 +/-   14.2425  usec
open    1048.1200 +/-   15.5097  usec
...
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
