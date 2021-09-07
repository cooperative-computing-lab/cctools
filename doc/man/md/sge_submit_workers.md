






















# sge_submit_workers(1)

## NAME
**sge_submit_workers** - submit work_queue_worker to a SUN Grid Engine (SGE).

## SYNOPSIS
**sge_submit_workers [options] _&lt;servername&gt;_ _&lt;port&gt;_ _&lt;num-workers&gt;_**

when auto mode is not enabled for the worker, or

**sge_submit_workers [options] _&lt;num-workers&gt;_**

when auto mode is enabled for the worker.

## DESCRIPTION
**sge_submit_workers** schedules the execution of [work_queue_worker(1)](work_queue_worker.md)
on the SUN Grid Engine (SGE) through its job submission interface, qsub.
The number of **work_queue_worker** scheduled and run is given by the **num-workers**
argument.

The **servername** and **port** arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. These two arguments become optional when the
auto mode option is specified for work_queue_worker.

## OPTIONS

- **-M**,**--manager-name=_&lt;name&gt;_**<br />Name of the preferred manager for worker. (auto mode enabled)
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server for work_queue_worker to _&lt;catalog&gt;_. _&lt;catalog&gt;_ format: HOSTNAME:PORT.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server for work_queue_worker to _&lt;catalog&gt;_. _&lt;catalog&gt;_ format: HOSTNAME:PORT.
- **-t**,**--timeout=_&lt;seconds&gt;_**<br />Abort work_queue_worker after this amount of idle time (default=900s).
- **-d**,**--debug=_&lt;subsystem&gt;_**<br />Enable debugging on worker for this subsystem (try -d all to start).
- **-w**,**--tcp-window-size=_&lt;size&gt;_**<br />Set TCP window size
- **-i**,**--min-backoff=_&lt;time&gt;_**<br />Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b**,**--max-backoff=_&lt;time&gt;_**<br />Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-z**,**--disk-threshold=_&lt;size&gt;_**<br />Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
- **-A**,**--arch=_&lt;arch&gt;_**<br />Set architecture string for the worker to report to manager instead of the value in uname.
- **-O**,**--os=_&lt;os&gt;_**<br />Set operating system string for the worker to report to manager instead of the value in uname.
- **-s**,**--workdir=_&lt;path&gt;_**<br />Set the location for creating the working directory of the worker.
- **-P**,**----password=_&lt;file&gt;_**<br />Password file to authenticate workers to manager.
- **--cores=_&lt;cores&gt;_**<br />Set the number of cores each worker should use (0=auto). (default=1)
- **--memory=_&lt;size&gt;_**<br />Manually set the amonut of memory (in MB) reported by this worker.
- **--disk=_&lt;size&gt;_**<br />Manually set the amount of disk (in MB) reported by this worker.
- **-j**<br />Use job array to submit workers.
- **--p** _&lt;parameters&gt;_<br />SGE qsub parameters.
- **-h**,**--help**<br />Show help message.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES

Submit 10 worker instances to run on SGE and connect to a specific manager:

```
sge_submit_workers manager.somewhere.edu 9123 10
```

Submit 10 work_queue_worker instances to run on SGE in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

```
sge_submit_workers -a -t 3600 -M Project_A 10
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 7.3.2 FINAL
