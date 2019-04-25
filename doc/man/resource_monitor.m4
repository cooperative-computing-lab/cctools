include(manual.h)dnl
HEADER(resource_monitor)

SECTION(NAME)
BOLD(resource_monitor) - monitors the cpu, memory, io, and disk usage of a tree of processes.

SECTION(SYNOPSIS)
CODE(BOLD(resource_monitor [options] -- command [command-options]))

SECTION(DESCRIPTION)

BOLD(resource_monitor) is a tool to monitor the computational
resources used by the process created by the command given as an
argument, and all its descendants.  The monitor works
'indirectly', that is, by observing how the environment changed
while a process was running, therefore all the information
reported should be considered just as an estimate (this is in
contrast with direct methods, such as ptrace). It works on
Linux, and can be used automatically by
CODE(makeflow) and CODE(work queue) applications.

Additionally, the user can specify maximum resource limits in the
form of a file, or a string given at the command line. If one of
the resources goes over the limit specified, then the monitor
terminates the task, and reports which resource went over the
respective limits.

In systems that support it, BOLD(resource_monitor) wraps some
libc functions to obtain a better estimate of the resources used.

Currently, the monitor does not support interactive applications. That
is, if a process issues a read call from standard input, and standard
input has not been redirected, then the tree process is
terminated. This is likely to change in future versions of the tool.

BOLD(resource_monitor) generates up to three log files: a summary file encoded
as json with the maximum values of resource used, a time-series that shows the
resources used at given time intervals, and a list of files that were opened
during execution.

The summary file is a JSON document with the following fields. Unless
indicated, all fields are an array with two values, a number that describes the
measurement, and a string describing the units (e.g.,  CODE([ measurement, "units" ])).

LONGCODE_BEGIN
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
LONGCODE_END

The time-series log has a row per time sample. For each row, the columns have the following meaning (all columns are integers):

LONGCODE_BEGIN
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
LONGCODE_END

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_TRIPLET(-d,debug,subsystem)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
OPTION_ITEM(`-v,--version')Show version string.
OPTION_ITEM(`-h,--help')Show help text.
OPTION_TRIPLET(-i,interval,n)Maximum interval between observations, in seconds (default=1).
OPTION_ITEM(--pid=pid)Track pid instead of executing a command line (warning: less precise measurements).
OPTION_ITEM(--accurate-short-processes)Accurately measure short running processes (adds overhead).
OPTION_TRIPLET(-c,sh,str)Read command line from CODE(str), and execute as '/bin/sh -c CODE(str)'.
OPTION_TRIPLET(-l,limits-file,file)Use maxfile with list of var: value pairs for resource limits.
OPTION_TRIPLET(-L,limits,string)String of the form `"var: value, var: value\' to specify resource limits. (Could be specified multiple times.)
OPTION_ITEM(`-f, --child-in-foreground')Keep the monitored process in foreground (for interactive use).
OPTION_TRIPLET(-O,with-output-files,template)Specify CODE(template) for log files (default=CODE(resource-pid)).
OPTION_ITEM(--with-time-series)Write resource time series to CODE(template.series).
OPTION_ITEM(--with-inotify)Write inotify statistics of opened files to default=CODE(template.files).
OPTION_TRIPLET(-V,verbatim-to-summary,str)Include this string verbatim in a line in the summary. (Could be specified multiple times.)
OPTION_ITEM(--measure-dir=dir)Follow the size of dir. By default the directory at the start of execution is followed. Can be specified multiple times. See --without-disk-footprint below.
OPTION_ITEM(--follow-chdir)Follow processes' current working directories.
OPTION_ITEM(--without-disk-footprint)Do not measure working directory footprint. Overrides --measure-dir.
OPTION_ITEM(--no-pprint)Do not pretty-print summaries.
OPTION_ITEM(--snapshot-events=file)Configuration file for snapshots on file patterns. See below.


OPTIONS_END

The limits file should contain lines of the form:

LONGCODE_BEGIN
resource: max_value
LONGCODE_END

It may contain any of the following fields, in the same units as
defined for the summary file:

CODE(max_concurrent_processes),
CODE(`wall_time, cpu_time'),
CODE(`virtual_memory, resident_memory, swap_memory'),
CODE(`bytes_read, bytes_written'),
CODE(`workdir_number_files_dirs, workdir_footprint')

SECTION(ENVIRONMENT VARIABLES)

LIST_BEGIN
LIST_ITEM(CODE(BOLD(CCTOOLS_RESOURCE_MONITOR_HELPER)) Location of the desired helper library to wrap libc calls. If not provided, a version of the helper library is packed with the resource_monitor executable.)
LIST_END

SECTION(EXIT STATUS)

LIST_BEGIN
LIST_ITEM 0 The command exit status was 0, and the monitor process ran without errors.
LIST_ITEM 1 The command exit status was non-zero, and the monitor process ran without errors.
LIST_ITEM 2 The command was terminated because it ran out of resources  (see options -l, -L).
LIST_ITEM 3 The command did not run succesfully because the monitor process had an error.
LIST_END

To obtain the exit status of the original command, see the generated file with extension CODE(.summary).


SECTION(SNAPSHOTS)

The resource_monitor  can be directed to take snapshots of the resources used
according to the files created by the processes monitored. The typical use of
monitoring snapshots is to set a watch on a log file, and generate a snapshot
when a line in the log matches a pattern. To activate the snapshot facility,
use the command line argument --snapshot-events=CODE(file), in which CODE(file) is a
JSON-encoded document with the following format:

LONGCODE_BEGIN
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
LONGCODE_END

All fields but BOLD(label) are optional. 

LIST_BEGIN
            LIST_ITEM FILENAME:                  Name of a file to watch.
            LIST_ITEM from-start:boolean         If FILENAME exits when the monitor starts running, process from line 1. Default: false, as monitored processes may be appending to already existing files.
            LIST_ITEM from-start-if-truncated    If FILENAME is truncated, process from line 1. Default: true, to account for log rotations.
            LIST_ITEM delete-if-found            Delete FILENAME when found. Default: false

            LIST_ITEM events:
LIST_BEGIN
            LIST_ITEM label        Name that identifies the snapshot. Only alphanumeric, -,
                         and _ characters are allowed. 
            LIST_ITEM on-create    Take a snapshot every time the file is created. Default: false
            LIST_ITEM on-delete    Take a snapshot every time the file is deleted. Default: false
            LIST_ITEM on-truncate  Take a snapshot when the file is truncated.    Default: false
            LIST_ITEM on-pattern   Take a snapshot when a line matches the regexp pattern.    Default: none
            LIST_ITEM count        Maximum number of snapshots for this label. Default: -1 (no limit)
LIST_END
LIST_END

The snapshots are recorded both in the main resource summary file under the key
BOLD(snapshots), and as a JSON-encoded document, with the extension
.snapshot.NN, in which NN is the sequence number of the snapshot. The snapshots
are identified with the key "snapshot_name", which is a comma separated string
of BOLD(label)`('count`)' elements. A label corresponds to a name that
identifies the snapshot, and the count is the number of times an event was
triggered since last check (several events may be triggered, for example, when
several matching lines are written to the log). Several events may have the same label, and exactly one of on-create, on-truncate, and on-pattern should be specified per event.


SECTION(EXAMPLES)

To monitor 'sleep 10', at 2 second intervals, with output to sleep-log.summary, and with a monitor alarm at 5 seconds:

LONGCODE_BEGIN
% resource_monitor --interval=2 -L"wall_time: 5" -o sleep-log -- sleep 10
LONGCODE_END

Execute 'date' and redirect its output to a file:

LONGCODE_BEGIN
% resource_monitor --sh 'date > date.output'
LONGCODE_END

It can also be run automatically from makeflow, by specifying the '-M' flag:

LONGCODE_BEGIN
% makeflow --monitor=some-log-dir Makeflow
LONGCODE_END

In this case, makeflow wraps every command line rule with the
monitor, and writes the resulting logs per rule in the
CODE(some-log-dir) directory

Additionally, it can be run automatically from Work Queue:

LONGCODE_BEGIN
q = work_queue_create_monitoring(port);
work_queue_enable_monitoring(q, some-log-dir, /*kill tasks on exhaustion*/ 1);
LONGCODE_END

wraps every task with the monitor and writes the resulting summaries in
CODE(some-log-file). 

SECTION(SNAPSHOTS EXAMPLES)

Generate a snapshot when "my.log" is created:

LONGCODE_BEGIN
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
LONGCODE_END

Generate snapshots every time a line is added to "my.log":

LONGCODE_BEGIN
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
LONGCODE_END

Generate snapshots on particular lines of "my.log":

LONGCODE_BEGIN
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
LONGCODE_END

The monitor can also generate a snapshot when a particular file is created. The
monitor can detected this file, generate a snapshot, and delete the file to get
ready for the next snapshot. In the following example the monitor takes a
snapshot everytime the file CODE(please-take-a-snapshot) is created:

LONGCODE_BEGIN
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
LONGCODE_END


SECTION(BUGS AND KNOWN ISSUES)

LIST_BEGIN
LIST_ITEM(The monitor cannot track the children of statically linked executables.)
LIST_ITEM(The option --snapshot-events assumes that the watched files are written by appending to them. File truncation may not be detected if between checks the size of the file is larger or equal to the size after truncation. File checks are fixed at intervals of 1 second.)
LIST_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
