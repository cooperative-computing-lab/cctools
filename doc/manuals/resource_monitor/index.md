# Resource Monitor User's Manual

## Overview

**resource_monitor** is a tool to monitor the computational resources used by
the process created by the command given as an argument, and all its
descendants. The monitor works *indirectly*, that is, by observing how the
environment changed while a process was running, therefore all the information
reported should be considered just as an estimate (this is in contrast with
direct methods, such as ptrace). It works on Linux, and can be used in stand-
alone mode, or automatically with [makeflow](../makeflow) and [work queue](../workqueue) applications.

**resource_monitor** generates up to three log files: a JSON encoded summary
file with the maximum values of resource used and the time they occurred, a
time-series that shows the resources used at given time intervals, and a list
of files that were opened during execution.

Additionally, **resource_monitor** may be set to produce measurement snapshots
according to events in some files (e.g., when a file is created, deleted, or a
regular expression pattern appears in the file.). Maximum resource limits can
be specified in the form of a file, or a string given at the command line. If
one of the resources goes over the limit specified, then the monitor
terminates the task, and reports which resource went over the respective
limits.

In systems that support it, **resource_monitor** wraps some libc functions to
obtain a better estimate of the resources used.

### Installing

See the [Installation Instructions](../install) for the Cooperative Computing Tools package.  Then, make sure to set your `PATH` appropriately.


## Running resource_monitor

On a terminal, type:

```sh
resource_monitor -O mymeasurements -- ls
```

This will generate the file mymeasurements.summary, with the resource
usage of the command `ls`. Further:  


```sh
resource_monitor -O mymeasurements --with-time-series --with-inotify -O mymeasurements -- ls
```

will generate three files describing the
resource usage of the command `ls`. These files are `mymeasurements.summary`
, ` mymeasurements.series`, and `mymeasurements.files`, in which PID
represents the corresponding process id. By default, measurements are taken
every second, and each time an event such as a file is opened, or a process
forks, or exits. We can specify the output names, and the sampling intervals:

```sh
resource_monitor -O log-sleep -i 2 -- sleep 10
```

The previous command will monitor `sleep 10`, at two second intervals, and will
generate the files `log-sleep.summary`, `log-sleep.series`, and
`log-sleep.files`. The monitor assumes that the application monitored is not
interactive.

To change this behaviour use the `-f` switch: 

```sh
resource_monitor -O my.summary -f -- /bin/sh
```

## Output Format

The summary is JSON encoded and includes the following fields:

|Field                    | Description    |
|-------------------------|----------------|
|command                  | the command line given as an argument. |
|start                    | time at start of execution, since the epoch. |
|end                      | time at end of execution, since the epoch. |
|exit_type                | one of `normal`, `signal` or `limit` (a string). |
|signal                   | number of the signal that terminated the process. Only present if exit_type is signal. |
|cores                    | maximum number of cores used. |
|cores_avg                | number of cores as cpu_time/wall_time. |
|exit_status              | final status of the parent process. |
|max_concurrent_processes | the maximum number of processes running concurrently. |
|total_processes          | count of all of the processes created. |
|wall_time                | duration of execution, end - start. |
|cpu_time                 | user+system time of the execution |
|virtual_memory           | maximum virtual memory across all processes. |
|memory                   | maximum resident size across all processes. |
|swap_memory              | maximum swap usage across all processes |
|bytes_read               | amount of data read from disk. |
|bytes_written            | amount of data written to disk. |
|bytes_received           | amount of data read from network interfaces. |
|bytes_sent               | amount of data written to network interfaces. |
|bandwidth                | maximum bandwidth used. |
|total_files              | total maximum number of files and directories of all the working directories in the tree. |
|disk                     | size of all working directories in the tree. |
|limits_exceeded          | resources over the limit with -l, -L options (JSON object). |
|peak_times               | seconds from start when a maximum occured (JSON object). |
|snapshots                | List of intermediate measurements, identified by snapshot_name (JSON object). |


The time-series log has a row per time sample. For each row, the columns have
the following meaning:

|Field                    | Description    |
|-------------------------|----------------|
|wall_clock               | the sample time, since the epoch, in microseconds. |
|cpu_time                 | accumulated user + kernel time, in microseconds. |
|cores                    | current number of cores used. |
|max_concurrent_processes | concurrent processes at the time of the sample. |
|virtual_memory           | current virtual memory size, in MB. |
|memory                   | current resident memory size, in MB. |
|swap_memory              | current swap usage, in MB. |
|bytes_read               | accumulated number of bytes read, in bytes. |
|bytes_written            | accumulated number of bytes written, in bytes. |
|bytes_received           | accumulated number of bytes received, in bytes. |
|bytes_sent               | accumulated number of bytes sent, in bytes. |
|bandwidth                | current bandwidth, in bps. |
|total_files              | current number of files and directories, across all working directories in the tree. |
|disk                     | current size of working directories in the tree, in MB. |


## Specifying Resource Limits

Resource limits can be specified with a JSON object in a file in the same
format as the  output format . Only resources specified in the file are
enforced. Thus, for example, to automatically kill a process after one hour,
or if it is using 5GB of swap, we can create the following file `limits.json`:

```json
{ "wall_time": [3600, "s"], "swap_memory": [5, "GB"] }
```

and set limits to the execution with:

```sh
resource_monitor -O output --monitor-limits=limits.json -- myapp `
```

## Snapshots

The **resource_monitor** can be directed to take snapshots of the resources used
according to the files created by the processes monitored. The typical use of
monitoring snapshots is to set a watch on a log file, and generate a snapshot
when a line in the log matches a pattern. 

Snapshots are specified via a JSON-encoded file with the following syntax:

```json
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
    },
    ...
```

|Field | Type | Description |
|------|------|-------------|
|FILENAME  |string  |Name of a text file to watch.
|from-start  |boolean  |If FILENAME exits when the monitor starts running, process from line 1. Default  |false, as monitored processes may be appending to already existing files.
|from-start-if-truncated  |boolean  |If FILENAME is truncated, process from line 1. Default  |true, to account for log rotations.
|delete-if-found boolean  |Delete FILENAME when found. Default  |false
|events  |array  |xxx 
|label  |string  |Name that identifies the snapshot. Only alphanumeric, -, and _ characters are allowed.
|on-create  |boolean  |Take a snapshot every time the file is created. Default  |false
|on-delete  |boolean  | Take a snapshot every time the file is deleted. Default  |false
|on-truncate  |boolean  | Take a snapshot when the file is truncated. Default  |false
|on-pattern  |boolean  | Take a snapshot when a line matches the regexp pattern. Default: none
|count  |integer  |Maximum number of snapshots for this label. Default  |-1 (no limit) 

All fields but *label* are optional.

As an example, assume that 'myapp' goes through three stages during execution:
start, processing, and analysis, and that it indicates the current stage by
writing a line to 'my.log' of the form '# STAGE'. We can direct the
*resource_monitor* to take a snapshot at the beginning of each stage as follows:

File: snapshots.json:

```json
{ "my.log": {
    "events":[
        { "label":"file-created", "on-creation":true },
        { "label":"started", "on-pattern":"^# START" },
        { "label":"end-of-start", "on-pattern":"^# PROCESSING" },
        { "label":"end-of-processing", "on-pattern":"^# ANALYSIS" },
        { "label":"file-deleted", "on-deletion":true }]}}
```

```sh
resource_monitor -O output --snapshots- file=snapshots.json -- myapp
```

Snapshots are included in the output summary
file as an array of JSON objects under the key `snapshots`. Additionally, each
snapshot is written to a file `output.snapshot.N`, where ` N ` is 0,1,2,...

As another example, the monitor can generate a snapshot every time a particular file is created. The monitor can detected this file, generate a snapshot, and delete the file to get ready for the next snapshot. In the following example the monitor takes a snapshot everytime the file please-take-a-snapshot is created:

```json
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

## Integration with other CCTools

###  Makeflow mode

If you already have a makeflow file, you can activate the resource_monitor by
giving the `--monitor` option to makeflow with a desired output directory, for
example:

```sh
makeflow --monitor monitor_logs Makeflow
```

In this case, makeflow wraps every command line rule with the monitor, and
writes the resulting logs per rule in the directory `monitor_logs`.

###  Work-queue mode

From Work Queue in python, monitoring is activated with:

```python
import work_queue as wq
q = wq.WorkQueue(port)
q.enable_monitoring()
```

Limits for a task are set by defining a `category` of tasks. All tasks in a
category are assumed to use a similar quantity of resources:

```
# creating a category by assigning maximum resources:
q.specify_category_max_resources('my-category', {"cores": 1, "memory":512})

t = wq.Task(...)
t.specify_category('my-category')

...

t = q.wait(5)


if t:
    print("cores:  {}".format(t.resources_measured.cores))
    print("memory: {}".format(t.resources_measured.memory))
    print("disk:   {}".format(t.resources_measured.disk))
    print("bytes_read:   {}".format(t.resources_measured.bytes_read))
    ...

    if t.limits_exceeded:
        # any resource above a specified limit is different from -1:
        if t.limits_exceeded.memory != -1:
            ...

```

Similarly, in C:

```c
q = work_queue_create(port);

/* wraps every task with the monitor, and appends all generated summary files
 * into the file `some-log- file. */
work_queue_enable_monitoring(q, "some-log-file", /* kill tasks on exhaustion */ 1);

...

struct work_queue_task *t = work_queue_wait(q, 5);

if(t) {
    /* access resources measured with t->resources_measured->{cores,disk,memory,...} */
    /* and limits exceeded with: */
    if(t->resources_measured->limits_exceeded) {
        if(t->resources_measured->limits_exceeded->cores != -1) { ... }
    }
}
```

##  Monitoring with Condor

Unlike the previous examples, when using the `resource_monitor` directly with
**condor** , you have to specify the `resource_monitor` as an input file, and
the generated log files as output files. For example, consider the following
submission file:

```conf
universe = vanilla
executable = matlab
arguments = -r "run script.m"
output = matlab.output
transfer_input_files=script.m
should_transfer_files = yes
when_to_transfer_output = on_exit
log = condor.matlab.logfile
queue
```

This can be rewritten for monitoring as:

```conf
universe = vanilla
executable = resource_monitor
arguments = -O matlab-resources --limits-file=limits.json -r "run script.m"
output = matlab.output
transfer_input_files=script.m,limits.json,/path/to/resource_monitor
transfer_output_files=matlab-resources.summary
should_transfer_files = yes
when_to_transfer_output = on_exit
log = condor.matlab.logfile queue
```

## Further Information

For more information, please see [Getting Help](../help) or visit the [Cooperative Computing Lab](http://ccl.cse.nd.edu) website.

## Copyright

CCTools is Copyright (C) 2019- The University of Notre Dame. This software is distributed under the GNU General Public License Version 2. See the file COPYING for
details.
