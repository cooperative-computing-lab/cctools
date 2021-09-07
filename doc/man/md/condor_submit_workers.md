






















# condor_submit_workers(1)

## NAME
**condor_submit_workers** - submit work_queue_worker to the Condor grid.

## SYNOPSIS
```
****condor_submit_workers [options] <servername> <port> <num-workers>****
```

or

```
****condor_submit_workers [options] --manager-name <name> <num-workers>****
```


## DESCRIPTION
**condor_submit_workers** schedules the execution of [work_queue_worker(1)](work_queue_worker.md)
on a grid managed by Condor through its job submission interface, condor_submit.
The number of **work_queue_worker** scheduled and run is given by the **num-workers**
argument.

The **servername** and **port** arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. Alternatively, this information can be obtained from
the catalog server by specifying the name of the work queue using the --manager-name parameter.

## OPTIONS

- **-M --manager-name <name>** Name of the preferred manager for worker.
- **-C --catalog <catalog>** Set catalog server to <catalog>. <catalog> format: HOSTNAME:PORT.
- **-t --timeout <time>** Abort after this amount of idle time (default=900s).
- **-d --debug <subsystem>** Enable debugging on worker for this subsystem (try -d all to start).
- **-w --tcp-window-size <size>** Set TCP window size
- **-i --min-backoff <time>** Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b --max-backoff <time>** Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-z --disk-threshold <size>** Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
- **-A --arch <arch>** Set architecture string for the worker to report to manager instead of the value in uname.
- **-O --os <os>** Set operating system string for the worker to report to manager instead of the value in uname.
- **-s --workdir <path>** Set the location for creating the working directory of the worker.
- **-P --password <file>** Password file to authenticate workers to manager.
- **-E --worker-options <<str>>** Extra options passed to work_queue_worker

- **--cores cores** Set the number of cores each worker should use (0=auto). (default=1)
- **--memory size** Manually set the amonut of memory (in MB) reported by this worker.
- **--disk size** Manually set the amount of disk (in MB) reported by this worker.

- **-r --requirements <<reqs>>** Condor requirements expression.
- **--class-ad <ad>** Extra condor class ad. May be specified multiple times.
- **--autosize** Condor will automatically size the worker to the slot.
- **--docker-universe <image>** Run worker inside <image> using condor's docker universe

- **-h,--help** Show help message.




## EXIT STATUS
On success, returns zero. On failure, returns non-zero.

## EXAMPLES

Submit 10 worker instances to run on Condor and connect to a specific manager:

```
condor_submit_workers manager.somewhere.edu 9123 10
```

Submit 10 work_queue_worker instances to run on Condor in auto mode with their
preferred project name set to Project_A and abort timeout set to 3600 seconds:

```
condor_submit_workers -a -t 3600 -M Project_A 10
```

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 7.3.2 FINAL
