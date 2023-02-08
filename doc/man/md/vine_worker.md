






















# vine_worker(1)

## NAME
**vine_worker** - worker process for executing tasks
dispatched through TaskVine

## SYNOPSIS
**vine_worker [options] _&lt;manager_host&gt;_ _&lt;manager_port&gt;_**

**vine_worker [options] _&lt;manager_host:manager_port]&gt;_**

**vine_worker [options] "_&lt;manager_host:manager_port;[manager_host:port;manager_host:port;...]&gt;_**)

**vine_worker [options] -M _&lt;projectname&gt;_**

## DESCRIPTION

**vine_worker** is the worker process for executing tasks dispatched
from a manager application built using the **TaskVine** API. **vine_worker**
connects to the manager application, accepts, runs, and returns tasks dispatched to it.
**vine_worker** caches data obtained from external services and output from recently
computed tasks and maintains it on disk to accelerate future applications.



The **managerhost** and **port** arguments specify the hostname and port
number of the manager application for vine_worker to connect. Several
managerhosts and ports may be specified, separated with a semicolon (;), with the
worker connecting to any of the managers specified (When specifying multiple
managers, remember to escape the ; from shell interpretation, for example, using
quotes.)

Alternatevely, the manager may be specified by name, using the **-M** option.



**vine_worker** can be run locally or deployed remotely on any of the
grid or cloud computing environments such as SGE, PBS, SLURM, and HTCondor using
[sge_submit_workers(1)](sge_submit_workers.md), [pbs_submit_workers(1)](pbs_submit_workers.md), [slurm_submit_workers()](slurm_submit_workers.md), and [condor_submit_workers(1)](condor_submit_workers.md) respectively.

## OPTIONS

- **-v**,**--version**<br />Show version string.
- **-h**,**--help**<br />Show this help message.
- **-M**,**--manager-name=_&lt;name&gt;_**<br />Set the name of the project this worker should work for.  A worker can have multiple projects.
- **-C**,**--catalog=_&lt;catalog&gt;_**<br />Set catalog server to _&lt;catalog&gt;_. Format: HOSTNAME:PORT
- **-d**,**--debug=_&lt;flag&gt;_**<br />Enable debugging for the given subsystem. Try -d all as a start.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--debug-max-rotate=_&lt;bytes&gt;_**<br />Set the maximum file size of the debug log.  If the log exceeds this size, it is renamed to "filename.old" and a new logfile is opened.  (default=10M. 0 disables)
- **--debug-release-reset**<br />Debug file will be closed, renamed, and a new one opened after being released from a manager.
- **-P**,**--password=_&lt;pwfile&gt;_**<br />Password file for authenticating to the manager.
- **-t**,**--timeout=_&lt;time&gt;_**<br />Abort after this amount of idle time. (default=900s)
- **--parent-death**<br />Exit if parent process dies.
- **-w**,**--tcp-window-size=_&lt;size&gt;_**<br />Set TCP window size.
- **-i**,**--min-backoff=_&lt;time&gt;_**<br />Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b**,**--max-backoff=_&lt;time&gt;_**<br />Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-A**,**--arch=_&lt;arch&gt;_**<br />Set the architecture string the worker reports to its supervisor. (default=the value reported by uname)
- **-O**,**--os=_&lt;os&gt;_**<br />Set the operating system string the worker reports to its supervisor. (default=the value reported by uname)
- **-s**,**--workdir=_&lt;path&gt;_**<br />Set the location where the worker should create its working directory. (default=/tmp). Also configurable through environment variables **CCTOOLS_TEMP** or **TMPDIR**.
- **--cores=_&lt;n&gt;_**<br />Set the number of cores this worker should use.  Set it to 0 to have the worker use all of the available resources. (default=1)
- **--gpus=_&lt;n&gt;_**<br />Set the number of GPUs this worker should use. If less than 0 or not given, try to detect gpus available.
- **--memory=_&lt;mb&gt;_**<br />Manually set the amount of memory (in MB) reported by this worker.
- **--disk=_&lt;mb&gt;_**<br />Manually set the amount of disk space (in MB) reported by this worker.
- **--wall-time=_&lt;s&gt;_**<br />Set the maximum number of seconds the worker may be active.
- **--feature=_&lt;feature&gt;_**<br />Specifies a user-defined feature the worker provides (option can be repeated).
- **--volatility=_&lt;chance&gt;_**<br />Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).
- **--connection-mode=_&lt;mode&gt;_**<br />When using -M, override manager preference to resolve its address. One of by_ip, by_hostname, or by_apparent_ip. Default is set by manager.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To run **vine_worker** to join a specific manager process running on host **manager.somewhere.edu** port 9123:
```
% vine_worker manager.somewhere.edu 9123
```

To run **vine_worker** in auto mode with debugging turned on for all subsystems and
to accept tasks only from a manager application with project name set to project_A:
```
% vine_worker -a -d all -M project_A
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [TaskVine User Manual]("../taskvine.html")
- [vine_worker(1)](vine_worker.md) [vine_status(1)](vine_status.md) [vine_factory(1)](vine_factory.md) [vine_graph_log(1)](vine_graph_log.md) 


CCTools
