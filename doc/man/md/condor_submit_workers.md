






















# condor_submit_workers(1)

## NAME
**condor_submit_workers** - submit work_queue_worker to the Condor grid.

## SYNOPSIS
**condor_submit_workers [options] _&lt;servername&gt;_ _&lt;port&gt;_ _&lt;num-workers&gt;_**

or

**condor_submit_workers [options] --manager-name _&lt;name&gt;_ _&lt;num-workers&gt;_**


## DESCRIPTION
**condor_submit_workers** schedules the execution of [work_queue_worker(1)](work_queue_worker.md)
on a grid managed by Condor through its job submission interface, condor_submit.
The number of **work_queue_worker** scheduled and run is given by the **num-workers**
argument.

The **servername** and **port** arguments specify the hostname and port number of the
manager for the work_queue_worker to connect. Alternatively, this information can be obtained from
the catalog server by specifying the name of the work queue using the --manager-name parameter.

## OPTIONS

- **-M**,**--manager-name=_&lt;name&gt;_**<br />Name of the preferred manager for worker.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. _&lt;catalog&gt;_ format: HOSTNAME:PORT.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. _&lt;catalog&gt;_ format: HOSTNAME:PORT.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Abort after this amount of idle time (default=900s).
- **-d**,**--debug=_&lt;subsystem&gt;_**<br />Enable debugging on worker for this subsystem (try -d all to start).
- **-w**,**--tcp-window-size=_&lt;size&gt;_**<br />Set TCP window size
- **-i**,**--min-backoff=_&lt;time&gt;_**<br />Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b**,**--max-backoff=_&lt;time&gt;_**<br />Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-z**,**--disk-threshold=_&lt;size&gt;_**<br />Set available disk space threshold (in MB). When exceeded worker will clean up and reconnect. (default=100MB)
- **-A**,**--arch=_&lt;arch&gt;_**<br />Set architecture string for the worker to report to manager instead of the value in uname.
- **-O**,**--os=_&lt;os&gt;_**<br />Set operating system string for the worker to report to manager instead of the value in uname.
- **-s**,**--workdir=_&lt;path&gt;_**<br />Set the location for creating the working directory of the worker.
- **-P**,**--password=_&lt;file&gt;_**<br />Password file to authenticate workers to manager.
- **-E**,**--worker-options=_&lt;str&gt;_**<br />Extra options passed to work_queue_worker

- **--cores=_&lt;cores&gt;_**<br />Set the number of cores each worker should use (0=auto). (default=1)
- **--memory=_&lt;size&gt;_**<br />Manually set the amonut of memory (in MB) reported by this worker.
- **--disk=_&lt;size&gt;_**<br />Manually set the amount of disk (in MB) reported by this worker.
- **--scratch-dir=_&lt;path&gt;_**<br />Set the scratch directory location created on the local machine. (default=/tmp/${USER}-workers)
- **-r**,**--requirements=_&lt;reqs&gt;_**<br />Condor requirements expression.
- **--class-ad=_&lt;ad&gt;_**<br />Extra condor class ad. May be specified multiple times.
- **--autosize**<br />Condor will automatically size the worker to the slot.
- **--docker-universe=_&lt;image&gt;_**<br />Run worker inside _&lt;image&gt;_ using condor's docker universe
- **--docker-universe=_&lt;image&gt;_**<br />Run worker inside _&lt;image&gt;_ using condor's docker universe

- **-h**,**--help**<br />Show help message.




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
The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [uge_submit_workers(1)](uge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools
