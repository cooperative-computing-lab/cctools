






















# confuga(1)

## NAME
**Confuga** - An active storage cluster file system.

## SYNOPSIS
**chirp_server --jobs --root=_&lt;Confuga URI&gt;_ [options]**

## DESCRIPTION


Configures and starts a Chirp server to act as the head node for a Confuga
storage cluster.


For complete details with examples, see the [Confuga User's Manual](http://ccl.cse.nd.edu/software/manuals/confuga.html).

## OPTIONS


A Chirp server acting as the Confuga head node uses normal
[chirp_server(1)](chirp_server.md) options. In order to run the Chirp server as the
Confuga head node, use the **--root** switch with the Confuga URI. You must
also enable job execution with the **--jobs** switch.


The format for the Confuga URI is:
**confuga:///path/to/workspace?option1=value&option2=value**. The workspace
path is the location Confuga maintains metadata and databases for the head
node. Confuga specific options are also passed through the URI, documented
below.  Examples demonstrating how to start Confuga and a small cluster are at
the end of this manual.


- **--auth=_&lt;method&gt;_**<br />Enable this method for Head Node to Storage Node authentication. The default is to enable all available authentication mechanisms.
- **--concurrency=_&lt;limit&gt;_**<br />Limits the number of concurrent jobs executed by the cluster. The default is 0 for limitless.
- **--pull-threshold=_&lt;bytes&gt;_**<br />Sets the threshold for pull transfers. The default is 128MB.
- **--replication=_&lt;type&gt;_**<br />Sets the replication mode for satisfying job dependencies. **type** may be **push-sync** or **push-async-N**. The default is **push-async-1**.
- **--scheduler=_&lt;type&gt;_**<br />Sets the scheduler used to assign jobs to storage nodes. The default is **fifo-0**.
- **--tickets=_&lt;tickets&gt;_**<br />Sets tickets to use for authenticating with storage nodes. Paths must be absolute.


## STORAGE NODES


Confuga uses regular Chirp servers as storage nodes. Each storage node is
added to the cluster using the [confuga_adm(1)](confuga_adm.md) command.  All storage
node Chirp servers must be run with:


- Ticket authentication enabled (**--auth=ticket**). Remember by default all authentication mechanisms are enabled.
- Job execution enabled (**--jobs**).
- Job concurrency of at least two (**--job-concurrency=2**).



These options are also suggested but not required:


- More frequent Catalog updates (**--catalog-update=30s**).
- Project name for the cluster (**--project-name=foo**).



You must also ensure that the storage nodes and the Confuga head node are using
the same [catalog_server(1)](catalog_server.md). By default, this should be the case. The
**EXAMPLES** section below includes an example cluster using a manually
hosted catalog server.

### ADDING STORAGE NODES


To add storage nodes to the Confuga cluster, use the [confuga_adm(1)](confuga_adm.md)
administrative tool.

## EXECUTING WORKFLOWS


The easiest way to execute workflows on Confuga is through [makeflow(1)](makeflow.md).
Only two options to Makeflow are required, **--batch-type** and
**--working-dir**. Confuga uses the Chirp job protocol, so the batch type is
**chirp**. It is also necessary to define the executing server, the Confuga
Head Node, and the _namespace_ the workflow executes in. For example:

```
makeflow --batch-type=chirp --working-dir=chirp://confuga.example.com:9094/**path/to/workflow**
```


The workflow namespace is logically prepended to all file paths defined in the
Makeflow specification. So for example, if you have this Makeflow file:

```
a: exe
    ./exe > a
```


Confuga will execute **/path/to/workflow/exe** and produce the output file **/path/to/workflow/a**.


Unlike other batch systems used with Makeflow, like Condor or Work Queue,
_all files used by a workflow must be in the Confuga file system_. Condor
and Work Queue both stage workflow files from the submission site to the
execution sites. In Confuga, the entire workflow dataset, including
executables, is already resident.  So when executing a new workflow, you need
to upload the workflow dataset to Confuga. The easiest way to do this is using
the [chirp(1)](chirp.md) command line tool:

```
chirp confuga.example.com put workflow/ /path/to/
```


Finally, Confuga does not save the _stdout_ or _stderr_ of jobs.
If you want these files for debugging purposes, you must explicitly save them.
To streamline the process, you may use Makeflow's **--wrapper** options to
save _stdout_ and _stderr_:

```
makeflow --batch-type=chirp \\
         --working-dir=chirp://confuga.example.com/ \\
         --wrapper=$'{\\n{}\\n} > stdout.%% 2> stderr.%%' \\
         --wrapper-output='stdout.%%' \\
         --wrapper-output='stderr.%%'
```

## EXAMPLES


Launch a head node with Confuga state stored in **./confuga.root**:

```
chirp_server --jobs --root="confuga://$(pwd)/confuga.root/"
```


Launch a head node with workspace **/tmp/confuga.root** using storage nodes **chirp://localhost:10001** and **chirp://localhost:10002/u/joe/confuga**:

```
chirp_server --jobs --root='confuga:///tmp/confuga.root/'
confuga_adm confuga:///tmp/confuga.root/ sn-add address localhost:10001
confuga_adm confuga:///tmp/confuga.root/ sn-add -r /u/joe/confuga address localhost:10001
```


Run a simple test cluster on your workstation:

```
# start a catalog server in the background
catalog_server --history=catalog.history \\
               --update-log=catalog.update \\
               --interface=127.0.0.1 \\
               &
# sleep for a time so catalog can start
sleep 1
# start storage node 1 in the background
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=10s \\
             --interface=127.0.0.1 \\
             --jobs \\
             --job-concurrency=10 \\
             --root=./root.1 \\
             --port=9001 \\
             --project-name=test \\
             --transient=./tran.1 \\
             &
# start storage node 2 in the background
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=10s \\
             --interface=127.0.0.1 \\
             --jobs \\
             --job-concurrency=10 \\
             --root=./root.2 \\
             --port=9002 \\
             --project-name=test \\
             --transient=./tran.2 \\
             &
# sleep for a time so catalog can receive storage node status
sleep 5
confuga_adm confuga:///$(pwd)/confuga.root/ sn-add address localhost:9001
confuga_adm confuga:///$(pwd)/confuga.root/ sn-add address localhost:9002
# start the Confuga head node
chirp_server --advertise=localhost \\
             --catalog-name=localhost \\
             --catalog-update=30s \\
             --debug=confuga \\
             --jobs \\
             --root="confuga://$(pwd)/confuga.root/?auth=unix" \\
             --port=9000
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO
[confuga_adm(1)](confuga_adm.md) 
- [Cooperative Computing Tools Documentation]("../index.html")
- [Chirp User Manual]("../chirp.html")
- [chirp(1)](chirp.md)  [chirp_status(1)](chirp_status.md)  [chirp_fuse(1)](chirp_fuse.md)  [chirp_get(1)](chirp_get.md)  [chirp_put(1)](chirp_put.md)  [chirp_stream_files(1)](chirp_stream_files.md)  [chirp_distribute(1)](chirp_distribute.md)  [chirp_benchmark(1)](chirp_benchmark.md)  [chirp_server(1)](chirp_server.md)


CCTools
