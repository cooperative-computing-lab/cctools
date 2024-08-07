






















# wavefront_master(1)

## NAME
**wavefront_master** - executes Wavefront workflow in parallel on distributed systems

## SYNOPSIS
**wavefront [options] _&lt;command&gt;_ _&lt;xsize&gt;_ _&lt;ysize&gt;_ _&lt;inputdata&gt;_ _&lt;outputdata&gt;_**

## DESCRIPTION

**wavefront_master** computes a two dimensional recurrence relation. You
provide a function F (**_&lt;command&gt;_**) that accepts the left (x), right
(y), and diagonal (d) values and initial values (**_&lt;inputdata&gt;_**) for
the edges of the matrix. The output matrix, whose size is determined by
**_&lt;xsize&gt;_** and **_&lt;ysize&gt;_**, will be stored in a file specified
by **_&lt;outputdata&gt;_**.

**wavefront_master** uses the Work Queue system to distribute tasks among
processors. After starting **wavefront_master**, you must start a number of
[work_queue_worker(1)](work_queue_worker.md) processes on remote machines.  The workers will
then connect back to the master process and begin executing tasks.

## OPTIONS


- **-h**,**--help**<br />Show this help screen
- **-v**,**--version**<br />Show version string
- **-d**,**--debug=_&lt;subsystem&gt;_**<br />Enable debugging for this subsystem. (Try -d all to start.)
- **-N**,**--project-name=_&lt;project&gt;_**<br />Set the project name to _&lt;project&gt;_
- **-N**,**--project-name=_&lt;project&gt;_**<br />Set the project name to _&lt;project&gt;_
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-p**,**--port=_&lt;port&gt;_**<br />Port number for queue master to listen on.
- **-P**,**--priority=_&lt;num&gt;_**<br />Priority. Higher the value, higher the priority.
- **-Z**,**--port-file=_&lt;file&gt;_**<br />Select port at random and write it to this file.  (default is disabled)
- **--work-queue-preferred-connection=_&lt;connection&gt;_**<br />Indicate preferred connection. Chose one of by_ip or by_hostname. (default is by_ip)


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Suppose you have a program named **function** that you want to use in the
Wavefont workflow computation. The program **function**, when invoked as
**function a b c**, should do some computations on files **a**, **b** and
**c** and produce some output on the standard output.

Before running **wavefront_master**, you need to create a file, say
**input.data**, that lists initial values of the matrix (values on the left
and bottom edges), one per line:

```
 0	0	value.0.0
 0	1	value.0.1
 ...
 0	n	value.0.n
 1	0	value.1.0
 2	0	value.2.0
 ...
 n	0	value.n.0
```

To run a Wavefront workflow sequentially, start a single
[work_queue_worker(1)](work_queue_worker.md) process in the background. Then, invoke
**wavefront_master**. The following example computes a 10 by 10 Wavefront
matrix:

```
 % work_queue_worker localhost 9123 &
 % wavefront_master function 10 10 input.data output.data
```

The framework will carry out the computations in the order of dependencies, and
print the results one by one (note that the first two columns are X and Y
indices in the resulting matrix) in the specified output file. Below is an
example of what the output file - **output.data** would look like:

```
 1	1	value.1.1
 1	2	value.1.2
 1	3	value.1.3
 ...
```

To speed up the process, run more [work_queue_worker(1)](work_queue_worker.md) processes on
other machines, or use [condor_submit_workers(1)](condor_submit_workers.md) or
[uge_submit_workers(1)](uge_submit_workers.md) to start hundreds of workers in your local batch
system.

The following is an example of adding more workers to execute a Wavefront
workflow. Suppose your **wavefront_master** is running on a machine named
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
- [Wavefront User Manual]("http://ccl.cse.nd.edu/software/manuals/wavefront.html")
- [Work Queue User Manual]("http://ccl.cse.nd.edu/software/manuals/workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md)
- [condor_submit_workers(1)](condor_submit_workers.md)
- [uge_submit_workers(1)](uge_submit_workers.md)


CCTools
