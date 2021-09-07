






















# work_queue_worker(1)

## NAME
**work_queue_worker** - worker process for executing tasks
dispatched through Work Queue

## SYNOPSIS
****work_queue_worker [options] <managerhost> <port>****

****work_queue_worker [options] <managerhost:port]>****

****work_queue_worker [options] "<managerhost:port;[managerhost:port;managerhost:port;...]>****"

****work_queue_worker [options] -M <projectname>****

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
grid or cloud computing environments such as SGE, PBS, SLURM, and HTCondor using
[sge_submit_workers(1)](sge_submit_workers.md), [pbs_submit_workers(1)](pbs_submit_workers.md), [slurm_submit_workers()](slurm_submit_workers.md), and [condor_submit_workers(1)](condor_submit_workers.md) respectively.

## OPTIONS

- **-v** Show version string.
- **-h** Show this help message.
- **-M --manager-name <name>** Set the name of the project this worker should work for.  A worker can have multiple projects.
- **-C --catalog <catalog>** Set catalog server to <catalog>. Format: HOSTNAME:PORT
- **-d --debug <flag>** Enable debugging for the given subsystem. Try -d all as a start.
- **-o --debug-file <file>** Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--debug-max-rotate bytes** Set the maximum file size of the debug log.  If the log exceeds this size, it is renamed to "filename.old" and a new logfile is opened.  (default=10M. 0 disables)
- **--debug-release-reset** Debug file will be closed, renamed, and a new one opened after being released from a manager.
- **--foreman** Enable foreman mode.
- **-f --foreman-name <name>** Set the project name of this foreman to <project>. Implies --foreman.
- **--foreman-port port[:highport]**  Set the port for the foreman to listen on.  If <highport> is specified the port is chosen from between <port> and <highport>. Implies --foreman.
- **-Z --foreman-port-file <file>** Select port to listen to at random and write to this file.  Implies --foreman.
- **-F --fast-abort <mult>** Set the fast abort multiplier for foreman (default=disabled).
- **--specify-log logfile** Send statistics about foreman to this file.
- **-P --password <pwfile>** Password file for authenticating to the manager.
- **-t --timeout <time>** Abort after this amount of idle time. (default=900s)
- **--parent-death** Exit if parent process dies.
- **-w --tcp-window-size <size>** Set TCP window size.
- **-i --min-backoff <time>** Set initial value for backoff interval when worker fails to connect to a manager. (default=1s)
- **-b --max-backoff <time>** Set maxmimum value for backoff interval when worker fails to connect to a manager. (default=60s)
- **-A --arch <arch>** Set the architecture string the worker reports to its supervisor. (default=the value reported by uname)
- **-O --os <os>** Set the operating system string the worker reports to its supervisor. (default=the value reported by uname)
- **-s --workdir <path>** Set the location where the worker should create its working directory. (default=/tmp)
- **--bandwidth mbps** Set the maximum bandwidth the foreman will consume in Mbps. (default=unlimited)
- **--cores n** Set the number of cores this worker should use.  Set it to 0 to have the worker use all of the available resources. (default=1)
- **--gpus n** Set the number of GPUs this worker should use. If less than 0 or not given, try to detect gpus available.
- **--memory mb** Manually set the amount of memory (in MB) reported by this worker.
- **--disk mb** Manually set the amount of disk space (in MB) reported by this worker.
- **--wall-time s** Set the maximum number of seconds the worker may be active.
- **--feature feature** Specifies a user-defined feature the worker provides (option can be repeated).
- **--volatility chance** Set the percent chance per minute that the worker will shut down (simulates worker failures, for testing only).


## FOREMAN MODE

**work_queue_worker** can also be run in **foreman** mode, in which it connects to a
manager as a worker while acting as a manager itself.  Any tasks the foreman receives from
its manager are sent to its subordinate worker processes.



**Foreman** mode is enabled by either specifying a port to listen on using the **--foreman --foreman-port <port>** option or by
setting the mode directly with the **--foreman --foreman-name <foreman_name>**
option.  The foreman works for the manager specified with the with the **-M <project name>** flag.

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

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Work Queue User Manual]("../workqueue.html")
- [work_queue_worker(1)](work_queue_worker.md) [work_queue_status(1)](work_queue_status.md) [work_queue_factory(1)](work_queue_factory.md) [condor_submit_workers(1)](condor_submit_workers.md) [sge_submit_workers(1)](sge_submit_workers.md) [torque_submit_workers(1)](torque_submit_workers.md) 


CCTools 7.3.2 FINAL
