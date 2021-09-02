






















# pbs_submit_workers(1)

## NAME
**pbs_submit_workers** - submit work_queue_worker to a PBS cluster.

## SYNOPSIS
****pbs_submit_workers [options] <servername> <port> <num-workers>****

## DESCRIPTION
**pbs_submit_workers** schedules the execution of [work_queue_worker(1)](work_queue_worker.md)
on the PBS batch system through its job submission interface, qsub.
The number of **work_queue_worker** scheduled and run is given by the **num-workers**
argument.

The **servername** and **port** arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. These two arguments become optional when the
auto mode option is specified for work_queue_worker.

## OPTIONS

- **-M** Name of the preferred manager for worker.
- **-c cores** Set the number of cores each worker should use (0=auto). (default=1)
- **-C catalog** Set catalog server for work_queue_worker to <catalog>. <catalog> format: HOSTNAME:PORT.
- **-t seconds** Abort work_queue_worker after this amount of idle time (default=900s).
- **-d subsystem** Enable debugging on worker for this subsystem (try -d all to start).
- **-w size** Set TCP window size
- **-i time** Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b time** Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-z size** Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
- **-A arch** Set architecture string for the worker to report to manager instead of the value in uname.
- **-O os** Set operating system string for the worker to report to manager instead of the value in uname.
- **-s path** Set the location for creating the working directory of the worker.
- **-j ** Use job array to submit workers.
- **-p parameters** PBS qsub parameters.
- **-h** Show help message.


## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES

Submit 10 worker instances to run on PBS and connect to a specific manager:

```
pbs_submit_workers manager.somewhere.edu 9123 10
```

Submit 10 work_queue_worker instances to run on PBS in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

```
pbs_submit_workers -a -t 3600 -M Project_A 10
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 8.0.0 DEVELOPMENT released on 
