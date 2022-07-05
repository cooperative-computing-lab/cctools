






















# chirp_server_hdfs(1)

## NAME
**chirp_server_hdfs** - run a chirp server with HDFS client setup

## SYNOPSIS
**chirp_server_hdfs [options]**

## DESCRIPTION


HDFS is the primary distributed filesystem used in the Hadoop project.
**chirp_server_hdfs** enables running **chirp_server** with the proper
environment variables set for using accessing HDFS. The command requires that
the **JAVA_HOME** and **HADOOP_HOME** environment variables be defined. Once
the environment is setup, the Chirp server will execute using HDFS as the backend
filesystem for all filesystem access.


For complete details with examples, see the [Chirp User's Manual](http://ccl.cse.nd.edu/software/manuals/chirp.html).

## OPTIONS

See [chirp_server(1)](chirp_server.md) for option listing.

## ENVIRONMENT VARIABLES


- **JAVA_HOME** Location of your Java installation.
- **HADOOP_HOME** Location of your Hadoop installation.



## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To start a Chirp server with a HDFS backend filesystem:

```
% chirp_server_hdfs -r hdfs://host:port/foo/bar
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
