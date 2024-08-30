






















# work_queue_worker(1)

## NAME
**work_queue_worker** - worker process for executing tasks
dispatched through Work Queue

## SYNOPSIS
**work_queue_worker [options] _&lt;manager_host&gt;_ _&lt;manager_port&gt;_**

**work_queue_worker [options] _&lt;manager_host:manager_port]&gt;_**

**work_queue_worker [options] "_&lt;manager_host:manager_port;[manager_host:manager_port;manager_host:manager_port;...]&gt;_**)

**work_queue_worker [options] -M _&lt;projectname&gt;_**

## DESCRIPTION

**work_queue_worker** is the worker process for executing tasks dispatched
from a manager application built using the **Work Queue** API. **work_queue_worker**
connects to the manager application, accepts, runs, and returns tasks dispatched to it.



The **managerhost** and **port** arguments specify the hostname and port
number of the manager application for work_queue_worker to connect. Several
managerhosts and ports may be specified, separated with a semicolon (;), with the
worker connecting to any of the managers specified (When specifying multiple
managers, remember to escape the ; from shell interpretation, for example, using
quotes.)

Alternatevely, the manager may be specified by name, using the **-M** option.



**work_queue_worker** can be run locally or deployed remotely on any of the
grid or cloud computing environments such as UGE, PBS, SLURM, and HTCondor using
[uge_submit_workers(1)](uge_submit_workers.md), [pbs_submit_workers(1)](pbs_submit_workers.md), [slurm_submit_workers()](slurm_submit_workers.md), and [condor_submit_workers(1)](condor_submit_workers.md) respectively.

## OPTIONS

- **-v**,**--version**<br />Show version string.
- **-h**,**--help**<br />Show this help message.
- **-M**,**--manager-name=_&lt;name&gt;_**<br />Set the name of the project this worker should work for.  A worker can have multiple projects.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. Format: HOSTNAME:PORT
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for the given subsystem. Try -d all as a start.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--debug-max-rotate=_&lt;bytes&gt;_**<br />Set the maximum file size of the debug log.  If the log exceeds this size, it is renamed to "filename.old" and a new logfile is opened.  (default=10M. 0 disables)
- **--debug-release-reset**<br />Debug file will be closed, renamed, and a new one opened after being released from a manager.
- **--foreman**<br />Enable foreman mode.
- **-f**,**--foreman-name=_&lt;name&gt;_**<br />Set the project name of this foreman to _&lt;project&gt;_. Implies --foreman.
- **--foreman-port=_&lt;port[:highport]&gt;_**<br /> Set the port for the foreman to listen on.  If _&lt;highport&gt;_ is specified the port is chosen from between _&lt;port&gt;_ and _&lt;highport&gt;_. Implies --foreman.
- **-Z**,**--foreman-port-file=_&lt;file&gt;_**<br />Select port to listen to at random and write to this file.  Implies --foreman.
- **-F**,**--fast-abort=_&lt;mult&gt;_**<br />Set the fast abort multiplier for foreman (default=disabled).
- **--specify-log=_&lt;logfile&gt;_**<br />Send statistics about foreman to this file.
- **-P**,**--password=_&lt;pwfile&gt;_**<br />Password file for authenticating to the manager.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Abort after this amount of idle time. (default=900s)
- **--parent-death**<br />Exit if parent process dies.
- **-w**,**--tcp-window-size=_&lt;size&gt;_**<br />Set TCP window size.
- **-i**,**--min-backoff=_&lt;time&gt;_**<br />Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b**,**--max-backoff=_&lt;time&gt;_**<br />Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-A**,**--arch=_&lt;arch&gt;_**<br />Set the architecture string the worker reports to its supervisor. (default=the value reported by uname)
- **-O**,**--os=_&lt;os&gt;_**<br />Set the operating system string the worker reports to its supervisor. (default=the value reported by uname)
- **-s**,**--workdir=_&lt;path&gt;_**<br />Set the location where the worker should create its working directory. (default=/tmp). Also configurable through environment variables **CCTOOLS_TEMP** or **TMPDIR**.
- **--bandwidth=_&lt;mbps&gt;_**<br />Set the maximum bandwidth the foreman will consume in Mbps. (default=unlimited)
- **--cores=_&lt;n&gt;_**<br />Set the number of cores this worker should use.  Set it to 0 to have the worker use all of the available resources. (default=1)
- **--gpus=_&lt;n&gt;_**<br />Set the number of GPUs this worker should use. If less than 0 or not given, try to detect gpus available.
- **--memory=_&lt;mb&gt;_**<br />Manually set the amount of memory (in MB) reported by this worker.
- **--disk=_&lt;mb&gt;_**<br />Manually set the amount of disk space (in MB) reported by this worker.
- **--wall-time=_&lt;s&gt;_**<br />Set the maximum number of seconds the worker may be active.
- **--feature=_&lt;feature&gt;_**<br />Specifies a user-defined feature the worker provides (option can be repeated).
- **--volatility=_&lt;chance&gt;_**<br />Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).
- **--connection-mode=_&lt;mode&gt;_**<br />When using -M, override manager preference to resolve its address. One of by_ip, by_hostname, or by_apparent_ip. Default is set by manager.


## FOREMAN MODE

**work_queue_worker** can also be run in **foreman** mode, in which it connects to a
manager as a worker while acting as a manager itself.  Any tasks the foreman receives from
its manager are sent to its subordinate worker processes.



**Foreman** mode is enabled by either specifying a port to listen on using the **--foreman --foreman-port _&lt;port&gt;_** option or by
setting the mode directly with the **--foreman --foreman-name _&lt;foreman_name&gt;_**
option.  The foreman works for the manager specified with the with the **-M _&lt;project name&gt;_** flag.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To run **work_queue_worker** to join a specific manager process running on host **manager.somewhere.edu** port 9123:
```
% work_queue_worker manager.somewhere.edu 9123
```

To run **work_queue_worker** in auto mode with debugging turned on for all subsystems and
to accept tasks only from a manager application with project name set to project_A:
```
% work_queue_worker -a -d all -M project_A
```

To run **work_queue_worker** as a foreman working for project_A and advertising itself as foreman_A1:
```
% work_queue_worker --foreman -M project_A -f foreman_A1
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [uge_submit_workers(1)](uge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools
