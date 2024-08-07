






















# allpairs_master(1)

## NAME
**allpairs_master** - executes All-Pairs workflow in parallel on distributed systems

## SYNOPSIS
**allparis_master [options] _&lt;set A&gt;_ _&lt;set B&gt;_ _&lt;compare function&gt;_**

## DESCRIPTION

**allpairs_master** computes the Cartesian product of two sets
(**_&lt;set A&gt;_** and **_&lt;set B&gt;_**), generating a matrix where each cell
M[i,j] contains the output of the function F (**_&lt;compare function&gt;_**) on
objects A[i] (an item in **_&lt;set A&gt;_**) and B[j] (an item in
**_&lt;set B&gt;_**). The resulting matrix is displayed on the standard output,
one comparison result per line along with the associated X and Y indices.

**allpairs_master** uses the Work Queue system to distribute tasks among
processors.  Each processor utilizes the [allpairs_multicore(1)](allpairs_multicore.md) program
to execute the tasks in parallel if multiple cores are present. After starting
**allpairs_master**, you must start a number of [work_queue_worker(1)](work_queue_worker.md)
processes on remote machines.  The workers will then connect back to the master
process and begin executing tasks.

## OPTIONS


- **-p**,**--port=_&lt;port&gt;_**<br />The port that the master will be listening on.
- **-e**,**--extra-args=_&lt;args&gt;_**<br />Extra arguments to pass to the comparison function.
- **-f**,**--input-file=_&lt;file&gt;_**<br />Extra input file needed by the comparison function. (may be given multiple times)
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-O**,**----output-file=_&lt;file&gt;_**<br />Write task output to this file (default to standard output)
- **-t**,**--estimated-time=_&lt;seconds&gt;_**<br />Estimated time to run one comparison. (default chosen at runtime)
- **-x**,**--width=_&lt;item&gt;_**<br />Width of one work unit, in items to compare. (default chosen at runtime)
- **-y**,**--height=_&lt;items&gt;_**<br />Height of one work unit, in items to compare. (default chosen at runtime)
- **-N**,**--project-name=_&lt;project&gt;_**<br />Report the master information to a catalog server with the project name - _&lt;project&gt;_
- **-P**,**--priority=_&lt;integer&gt;_**<br />Priority. Higher the value, higher the priority.
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for this subsystem. (Try -d all to start.)
- **-v**,**--version**<br />Show program version.
- **-h**,**--help**<br />Display this message.
- **-Z**,**--port-file=_&lt;file&gt;_**<br />Select port at random and write it to this file.  (default is disabled)
- **--work-queue-preferred-connection=_&lt;connection&gt;_**<br />Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Let's suppose you have a whole lot of files that you want to compare all to
each other, named **a**, **b**, **c**, and so on. Suppose that you also
have a program named **compareit** that when invoked as **compareit a b**
will compare files **a** and **b** and produce some output summarizing the
difference between the two, like this:

```
 a b are 45 percent similar
```

To use the allpairs framework, create a file called **set.list** that lists each of
your files, one per line:

```
 a
 b
 c
 ...
```

Because **allpairs_master** utilizes [allpairs_multicore(1)](allpairs_multicore.md), so please
make sure [allpairs_multicore(1)](allpairs_multicore.md) is in your PATH before you proceed.To run
a All-Pairs workflow sequentially, start a single [work_queue_worker(1)](work_queue_worker.md)
process in the background. Then, invoke **allpairs_master**.

```
 % work_queue_worker localhost 9123 &
 % allpairs_master set.list set.list compareit
```

The framework will carry out all possible comparisons of the objects, and print
the results one by one (note that the first two columns are X and Y indices in
the resulting matrix):

```
 1	1	a a are 100 percent similar
 1	2	a b are 45 percent similar
 1	3	a c are 37 percent similar
 ...
```

To speed up the process, run more [work_queue_worker(1)](work_queue_worker.md) processes on
other machines, or use [condor_submit_workers(1)](condor_submit_workers.md) or
[uge_submit_workers(1)](uge_submit_workers.md) to start hundreds of workers in your local batch
system.

The following is an example of adding more workers to execute a All-Pairs
workflow. Suppose your **allpairs_master** is running on a machine named
barney.nd.edu. If you have access to login to other machines, you could simply
start worker processes on each one, like this:

```
 % work_queue_worker barney.nd.edu 9123
```

If you have access to a batch system like Condor, you can submit multiple
workers at once:

```
 % condor_submit_workers barney.nd.edu 9123 10
 Submitting job(s)..........
 Logging submit event(s)..........
 10 job(s) submitted to cluster 298.
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [The Cooperative Computing Tools]("http://ccl.cse.nd.edu/software/manuals")
- [All-Pairs User Manual]("http://ccl.cse.nd.edu/software/manuals/allpairs.html")
- [Work Queue User Manual]("http://ccl.cse.nd.edu/software/manuals/workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md)
- [condor_submit_workers(1)](condor_submit_workers.md)
- [uge_submit_workers(1)](uge_submit_workers.md)
- [allpairs_multicore(1)](allpairs_multicore.md)


CCTools
