![](../logos/resource-monitor-logo.png)

# Resource Monitor User's Manual

## Overview

**resource_monitor** is a tool to monitor the computational resources used by
the process created by the command given as an argument, and all its
descendants. The monitor works *indirectly*, that is, by observing how the
environment changed while a process was running, therefore all the information
reported should be considered just as an estimate (this is in contrast with
direct methods, such as ptrace). It works on Linux, and it can be used in three ways:

- Stand alone mode, directly calling the `resource_monitor` executable.
- Activating the monitoring modes of [makeflow](../makeflow/index.md) and [work queue](../work_queue/index.md) applications.
- As a [python module](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceresource__monitor.html) to monitor single function evaluations.

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

See the [Installation Instructions](../install/index.md) for the Cooperative Computing Tools package.  Then, make sure to set your `PATH` appropriately.


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
|cores                    | maximum number of cores used in a small time window. |
|cores_avg                | number of cores as cpu_time/wall_time. |
|gpus                     | maximum number of gpus used. |
|exit_status              | final status of the parent process. |
|max_concurrent_processes | the maximum number of processes running concurrently. |
|total_processes          | count of all of the processes created. |
|wall_time                | duration of execution, end - start. |
|cpu_time                 | user+system time of the execution |
|virtual_memory           | maximum virtual memory across all processes. |
|memory                   | maximum resident size across all processes. |
|swap_memory              | maximum swap usage across all processes |
|bytes_read               | amount of data read from disk. (in MB)|
|bytes_written            | amount of data written to disk. (in MB)|
|bytes_received           | amount of data read from network interfaces. (in MB)|
|bytes_sent               | amount of data written to network interfaces. (in MB)|
|bandwidth                | maximum bandwidth used. (in Mbps) |
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

|Field | Type | Description | Default |
|------|------|-------------|---------|
|FILENAME  |string  |Name of a text file to watch.
|from-start  |boolean  |If FILENAME exits when the monitor starts running, process from line 1. | false |
|from-start-if-truncated  |boolean  |If FILENAME is truncated, process from line 1. | true |
|delete-if-found |boolean  |Delete FILENAME when found. | false|
|events  |array  | See following table ||

Events fields:

|Field | Type | Description | Default |
|------|------|-------------|---------|
|label  |string  |Name that identifies the snapshot. Only alphanumeric, -, and _ characters are allowed.| required|
|on-create  |boolean  |Take a snapshot every time the file is created. |false|
|on-delete  |boolean  | Take a snapshot every time the file is deleted. |false|
|on-truncate  |boolean  | Take a snapshot when the file is truncated. |false|
|on-pattern  |boolean  | Take a snapshot when a line matches the regexp pattern. ||
|count  |integer  |Maximum number of snapshots for this label. |-1 (no limit)|

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

## Monitoring functions in python

With the `resource_monitor` [python
module](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceresource__monitor.html)
python module, function evaluations can be monitored with resource limits
enforcement.

To monitor already defined functions, use the `monitored` function. This
creates a new function that returns a tuple of the original result, and a
dictionary of the resources used:

```python
import resource_monitor

def my_sum(a,b):
    return a + b

my_sum_monitored = resource_monitor.monitored()(my_sum)

original_result = my_sum(1,2)

(monitored_result, resources) = my_sum_monitored(1,2)
print('function used ' + str(resources['cores']) + ' cores')

assert(original_result, monitored_result)
```

Or more directly, use it as decorator:
```python
import resource_monitor

@resource_monitor.monitored()
def my_sum_decorated(a,b):
    return a + b

(monitored_result, resources) = my_sum_decorated(1,2)
```

With the function `resource_monitor.monitored`, we can specify resource limits
to be enforced. For example, if we simply want to enforce for a function not to
use for more than a megabyte of memory:

```python
import resource_monitor

@resource_monitor.monitored(limits = 'memory': 1024, return_resources = False)
def my_sum_limited(a,b):
    return a + b

try:
    # Note that since we used return_resources = False, the return value of the
    # function is not modified:
    x = my_sum_limited(1,2)
except resource_monitor.ResourceExhaustion as e:
    print(e.resources.limits_exceeded)
```

For a list of all the resources that can be monitored and enforced, please consult the documentation of the [module](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceresource__monitor.html).


Further, a function callback can be specified. This callback will be executed
at each measurement. As an example, we can use a callback to send messages to a
server with the resources measured:

```python
import resource_monitor

# monitor callback function example
# a callback function will be called everytime resources are measured.
# arguments are:
# - id:        unique identifier for the function invocation
# - fun_name:  string with the name of the function
# - step:      resource sample number (1 for the first, 2 for the second, ..., -1 for the last)
# - resources: dictionary with resources measured
def send_udp_message(id, fun_name, step, resources):
    """ Send a UDP message with the results of a measurement. """
    import socket
    import json

    finished   = True if step == -1 else False
    exhaustion = True if resources.get('limits_exceeded', False) else False

    msg = {'id': id, 'function': fun_name, 'finished': finished, 'resource_exhaustion': exhaustion, 'resources': resources}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(json.dumps(msg).encode(), ('localhost', 9800))

# Create the monitored function addint the callback. Also, set the interval measurements to 5s, instead of the default 1s
@resource_monitor.monitored(callback = send_udp_message, interval = 5, return_resources = False)
def my_function_monitored(...):
    ...

my_function_monitored(...)

# in another script, run the server as:
import socket
import pickle

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.bind(('localhost', 9800))

while True:
    data, addr = sock.recvfrom(1024)
    print("message: ", pickle.loads(data))
```

!!! warning
    The monitored function and the callback are executed in a different process
    from the calling environment. This means that they cannot modify variables
    from the calling environment.

For example, the following will not work as you may expect:

```python
# Note: This code does not work!!!

import resource_monitor

function_has_run = False
resources_series = []

def my_callback(id, fun_name, step, resources):
    resources_series.append(resources)

@resource_monitor.monitored(callback = my_callback):
def my_function():
    function_has_run = True

my_function()

# here function_has_run is still False, resources_series is [].
```

Please see an example
[here](http://ccl.cse.nd.edu/software/manuals/api/html/namespaceresource__monitor.html)
that shows how to construct a time series of the resources, and makes it
available to the calling environment.


## Further Information

For more information, please see [Getting Help](../help.md) or visit the [Cooperative Computing Lab](http://ccl.cse.nd.edu) website.

## Copyright

CCTools is Copyright (C) 2022 The University of Notre Dame. This software is distributed under the GNU General Public License Version 2. See the file COPYING for
details.
