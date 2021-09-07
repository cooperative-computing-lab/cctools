






















# resource_monitor(1)

## NAME
**resource_monitor** - monitors the cpu, memory, io, and disk usage of a tree of processes.

## SYNOPSIS
**resource_monitor [options] -- command [command-options]**

## DESCRIPTION

**resource_monitor** is a tool to monitor the computational
resources used by the process created by the command given as an
argument, and all its descendants.  The monitor works
'indirectly', that is, by observing how the environment changed
while a process was running, therefore all the information
reported should be considered just as an estimate (this is in
contrast with direct methods, such as ptrace). It works on
Linux, and can be used automatically by
**makeflow** and **work queue** applications.

Additionally, the user can specify maximum resource limits in the
form of a file, or a string given at the command line. If one of
the resources goes over the limit specified, then the monitor
terminates the task, and reports which resource went over the
respective limits.

In systems that support it, **resource_monitor** wraps some
libc functions to obtain a better estimate of the resources used.

Currently, the monitor does not support interactive applications. That
is, if a process issues a read call from standard input, and standard
input has not been redirected, then the tree process is
terminated. This is likely to change in future versions of the tool.

**resource_monitor** generates up to three log files: a summary file encoded
as json with the maximum values of resource used, a time-series that shows the
resources used at given time intervals, and a list of files that were opened
during execution.

The summary file is a JSON document with the following fields. Unless
indicated, all fields are an array with two values, a number that describes the
measurement, and a string describing the units (e.g.,  **[ measurement**).

```
command:                  the command line given as an argument
start:                    time at start of execution, since the epoch
end:                      time at end of execution, since the epoch
exit_type:                one of "normal", "signal" or "limit" (a string)
signal:                   number of the signal that terminated the process
                          Only present if exit_type is signal
cores:                    maximum number of cores used
cores_avg:                number of cores as cpu_time/wall_time
exit_status:              final status of the parent process
max_concurrent_processes: the maximum number of processes running concurrently
total_processes:          count of all of the processes created
wall_time:                duration of execution, end - start
cpu_time:                 user+system time of the execution
virtual_memory:           maximum virtual memory across all processes
memory:                   maximum resident size across all processes
swap_memory:              maximum swap usage across all processes
bytes_read:               amount of data read from disk
bytes_written:            amount of data written to disk
bytes_received:           amount of data read from network interfaces
bytes_sent:               amount of data written to network interfaces
bandwidth:                maximum bandwidth used
total_files:              total maximum number of files and directories of
                          all the working directories in the tree
disk:                     size of all working directories in the tree
limits_exceeded:          resources over the limit with -l, -L options (JSON object)
peak_times:               seconds from start when a maximum occured (JSON object)
snapshots:                List of intermediate measurements, identified by
                          snapshot_name (JSON object)
```

The time-series log has a row per time sample. For each row, the columns have the following meaning (all columns are integers):

```
wall_clock                the sample time, since the epoch, in microseconds
cpu_time                  accumulated user + kernel time, in microseconds
cores                     current number of cores used
max_concurrent_processes  concurrent processes at the time of the sample
virtual_memory            current virtual memory size, in MB
memory                    current resident memory size, in MB
swap_memory               current swap usage, in MB
bytes_read                accumulated number of bytes read, in bytes
bytes_written             accumulated number of bytes written, in bytes
bytes_received            accumulated number of bytes received, in bytes
bytes_sent                accumulated number of bytes sent, in bytes
bandwidth                 current bandwidth, in bps
total_files               current number of files and directories, across all
                          working directories in the tree
disk                      current size of working directories in the tree, in MB
```

## OPTIONS

- **-d**,**--debug=_&lt;subsystem&gt;_**<br />Enable debugging for this subsystem.
- **-o**,**--debug-file=_&lt;file&gt;_**<br />Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **-v**,**--version**<br />Show version string.
- **-h**,**--help**<br />Show help text.
- **-i**,**--interval=_&lt;n&gt;_**<br />Maximum interval between observations, in seconds (default=1).
- **--pid=_&lt;pid&gt;_**<br />Track pid instead of executing a command line (warning: less precise measurements).
- **--accurate-short-processes**<br />Accurately measure short running processes (adds overhead).
- **-c**,**--sh=_&lt;str&gt;_**<br />Read command line from **str**, and execute as '/bin/sh -c **str**'.
- **-l**,**--limits-file=_&lt;file&gt;_**<br />Use maxfile with list of var: value pairs for resource limits.
- **-L**,**--limits=_&lt;string&gt;_**<br />String of the form "var: value, var: value" to specify resource limits. (Could be specified multiple times.)
- **-f**,**--child-in-foreground**<br />Keep the monitored process in foreground (for interactive use).
- **--O**,**--with-output-files=_&lt;template&gt;_**<br />Specify **template** for log files (default=**resource-pid**).
- **--with-time-series**<br />Write resource time series to **template.series**.
- **--with-inotify**<br />Write inotify statistics of opened files to default=**template.files**.
- **-V**,**--verbatim-to-summary=_&lt;str&gt;_**<br />Include this string verbatim in a line in the summary. (Could be specified multiple times.)
- **--measure-dir=_&lt;dir&gt;_**<br />Follow the size of dir. By default the directory at the start of execution is followed. Can be specified multiple times. See --without-disk-footprint below.
- **--follow-chdir**<br />Follow the current working directories of the processes tree.
- **--without-disk-footprint**<br />Do not measure working directory footprint. Overrides --measure-dir.
- **--no-pprint**<br />Do not pretty-print summaries.
- **--snapshot-events=_&lt;file&gt;_**<br />Configuration file for snapshots on file patterns. See below.
- **--catalog-task-name=_&lt;task-name&gt;_**<br />Report measurements to catalog server with "task"=_&lt;task-name&gt;_.
- **--catalog-project=_&lt;project&gt;_**<br />Set project name of catalog update to _&lt;project&gt;_ (default=_&lt;task-name>&gt;_).
- **--catalog=_&lt;catalog&gt;_**<br />Use catalog server _&lt;catalog&gt;_. (default=catalog.cse.nd.edu:9097).
- **--catalog-interval=_&lt;interval&gt;_**<br />Send update to catalog every _&lt;interval&gt;_ seconds. (default=30).
- **--catalog-interval=_&lt;interval&gt;_**<br />Send update to catalog every _&lt;interval&gt;_ seconds. (default=30).



The limits file should contain lines of the form:

```
resource: max_value
```

It may contain any of the following fields, in the same units as
defined for the summary file:

**max_concurrent_processes**,
**wall_time, cpu_time**,
**virtual_memory, resident_memory, swap_memory**,
**bytes_read, bytes_written**,
**workdir_number_files_dirs, workdir_footprint**

## ENVIRONMENT VARIABLES


- **CCTOOLS_RESOURCE_MONITOR_HELPER** Location of the desired helper library to wrap libc calls. If not provided, a version of the helper library is packed with the resource_monitor executable.)


## EXIT STATUS


-  0 The command exit status was 0, and the monitor process ran without errors.
-  1 The command exit status was non-zero, and the monitor process ran without errors.
-  2 The command was terminated because it ran out of resources  (see options -l, -L).
-  3 The command did not run succesfully because the monitor process had an error.


To obtain the exit status of the original command, see the generated file with extension **.summary**.


## SNAPSHOTS

The resource_monitor  can be directed to take snapshots of the resources used
according to the files created by the processes monitored. The typical use of
monitoring snapshots is to set a watch on a log file, and generate a snapshot
when a line in the log matches a pattern. To activate the snapshot facility,
use the command line argument --snapshot-events=**file**, in which **file** is a
JSON-encoded document with the following format:

```
    {
        "FILENAME": {
            "from-start":boolean,
            "from-start-if-truncated":boolean,
            "delete-if-found":boolean,
            "events": [
                {
                    "label":"EVENT_NAME",
                    "on-create":boolean,
                    "on-truncate":boolean,
                    "on-pattern":"REGEXP",
                    "count":integer
                },
                {
                    "label":"EVENT_NAME",
                    ...
                }
            ]
        },
        "FILENAME": {
            ...
    }
```

All fields but **label** are optional. 


            -  FILENAME:                  Name of a file to watch.
            -  from-start:boolean         If FILENAME exits when the monitor starts running, process from line 1. Default: false, as monitored processes may be appending to already existing files.
            -  from-start-if-truncated    If FILENAME is truncated, process from line 1. Default: true, to account for log rotations.
            -  delete-if-found            Delete FILENAME when found. Default: false

            -  events:

            -  label        Name that identifies the snapshot. Only alphanumeric, -,
                         and _ characters are allowed. 
            -  on-create    Take a snapshot every time the file is created. Default: false
            -  on-delete    Take a snapshot every time the file is deleted. Default: false
            -  on-truncate  Take a snapshot when the file is truncated.    Default: false
            -  on-pattern   Take a snapshot when a line matches the regexp pattern.    Default: none
            -  count        Maximum number of snapshots for this label. Default: -1 (no limit)



The snapshots are recorded both in the main resource summary file under the key
**snapshots**, and as a JSON-encoded document, with the extension
.snapshot.NN, in which NN is the sequence number of the snapshot. The snapshots
are identified with the key "snapshot_name", which is a comma separated string
of **label**(count) elements. A label corresponds to a name that
identifies the snapshot, and the count is the number of times an event was
triggered since last check (several events may be triggered, for example, when
several matching lines are written to the log). Several events may have the same label, and exactly one of on-create, on-truncate, and on-pattern should be specified per event.


## EXAMPLES

To monitor 'sleep 10', at 2 second intervals, with output to sleep-log.summary, and with a monitor alarm at 5 seconds:

```
% resource_monitor --interval=2 -L"wall_time: 5" -o sleep-log -- sleep 10
```

Execute 'date' and redirect its output to a file:

```
% resource_monitor --sh 'date > date.output'
```

It can also be run automatically from makeflow, by specifying the '-M' flag:

```
% makeflow --monitor=some-log-dir Makeflow
```

In this case, makeflow wraps every command line rule with the
monitor, and writes the resulting logs per rule in the
**some-log-dir** directory

Additionally, it can be run automatically from Work Queue:

```
q = work_queue_create_monitoring(port);
work_queue_enable_monitoring(q, some-log-dir, /*kill tasks on exhaustion*/ 1);
```

wraps every task with the monitor and writes the resulting summaries in
**some-log-file**. 

## SNAPSHOTS EXAMPLES

Generate a snapshot when "my.log" is created:

```
{
    "my.log":
        {
            "events":[
                {
                    "label":"MY_LOG_STARTED",
                    "on-create:true
                }
            ]
        }
}
```

Generate snapshots every time a line is added to "my.log":

```
{
    "my.log":
        {
            "events":[
                {
                    "label":"MY_LOG_LINE",
                    "on-pattern":"^.*$"
                }
            ]
        }
}
```

Generate snapshots on particular lines of "my.log":

```
{
    "my.log":
        {
            "events":[
                {
                    "label":"started",
                    "on-pattern":"^# START"
                },
                {
                    "label":"end-of-start",
                    "on-pattern":"^# PROCESSING"
                }
                {
                    "label":"end-of-processing",
                    "on-pattern":"^# ANALYSIS"
                }
            ]
        }
}
```

The monitor can also generate a snapshot when a particular file is created. The
monitor can detected this file, generate a snapshot, and delete the file to get
ready for the next snapshot. In the following example the monitor takes a
snapshot everytime the file **please-take-a-snapshot** is created:

```
{
    "please-take-a-snapshot":
        {
            "delete-if-found":true,
            "events":[
                {
                    "label":"manual-snapshot",
                    "on-create":true
                }
            ]
        }
}
```


## BUGS AND KNOWN ISSUES


- The monitor cannot track the children of statically linked executables.
- The option --snapshot-events assumes that the watched files are written by appending to them. File truncation may not be detected if between checks the size of the file is larger or equal to the size after truncation. File checks are fixed at intervals of 1 second.


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools 7.3.2 FINAL
