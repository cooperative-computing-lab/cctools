include(manual.h)dnl
HEADER(resource_monitor)

SECTION(NAME)
BOLD(resource_monitor, resource_monitorv) - monitors the cpu, memory, io, and disk usage of a tree of processes.

SECTION(SYNOPSIS)
CODE(BOLD(resource_monitor [options] -- command [command-options]))

CODE(BOLD(resource_monitorv [options] -- command [command-options]))

SECTION(DESCRIPTION)

BOLD(resource_monitor) is a tool to monitor the computational
resources used by the process created by the command given as an
argument, and all its descendants.  The monitor works
'indirectly', that is, by observing how the environment changed
while a process was running, therefore all the information
reported should be considered just as an estimate (this is in
contrast with direct methods, such as ptrace). It has been tested
in Linux, FreeBSD, and Darwin, and can be used automatically by
CODE(makeflow) and CODE(work queue) applications.

Additionally, the user can specify maximum resource limits in the
form of a file, or a string given at the command line. If one of
the resources goes over the limit specified, then the monitor
terminates the task, and reports which resource went over the
respective limits.

In systems that support it, BOLD(resource_monitor) wraps some
libc functions to obtain a better estimate of the resources used.
In contrast, BOLD(resource_monitorv) disables this wrapping,
which means, among others, that it can only monitor the root
process, but not its descendants.

Currently, the monitor does not support interactive applications. That
is, if a process issues a read call from standard input, and standard
input has not been redirected, then the tree process is
terminated. This is likely to change in future versions of the tool.

BOLD(resource_monitor) generates up to three log files: a summary
file with the maximum values of resource used, a time-series that
shows the resources used at given time intervals, and a list of
files that were opened during execution.

The summary file has the following format:

LONGCODE_BEGIN
command:                   [the command line given as an argument]
start:                     [seconds at the start of execution, since the epoch, float]
end:                       [seconds at the end of execution, since the epoch,   float]
exit_type:                 [one of normal, signal or limit,                    string]
signal:                    [number of the signal that terminated the process.
                            Only present if exit_type is signal                   int]
limits_exceeded:           [resources over the limit. Only present if
                            exit_type is limit,                                string]
exit_status:               [final status of the parent process,                   int]
max_concurrent_processes:  [the maximum number of processes running concurrently, int]
total_processes:           [count of all of the processes created,                int]
wall_time:                 [seconds spent during execution, end - start,        float]
cpu_time:                  [user + system time of the execution, in seconds,    float]
virtual_memory:            [maximum virtual memory across all processes, in MB,   int]
resident_memory:           [maximum resident size across all processes, in MB,    int]
swap_memory:               [maximum swap usage across all processes, in MB,       int]
bytes_read:                [number of bytes read from disk,                       int]
bytes_written:             [number of bytes written to disk,                      int]
workdir_num_files:         [total maximum number of files and directories of 
                            all the working directories in the tree,              int]
workdir_footprint:         [size in MB of all working directories in the tree,    int]
LONGCODE_END

The time-series log has a row per time sample. For each row, the columns have the following meaning:

LONGCODE_BEGIN
wall_clock                [the sample time, since the epoch, in microseconds,      int]
cpu_time                  [accumulated user + kernel time, in microseconds,        int]
concurrent                [concurrent processes at the time of the sample,         int] 
virtual                   [current virtual memory size, in MB,                     int]
resident                  [current resident memory size, in MB,                    int]   
swap                      [current swap usage, in MB,                              int]   
bytes_read                [accumulated number of bytes read,                       int]
bytes_written             [accumulated number of bytes written,                    int]
files                     [current number of files and directories, across all
                           working directories in the tree,                        int]
footprint                 [current size of working directories in the tree, in MB  int]
LONGCODE_END

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_TRIPLET(-d,debug,subsystem)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Send debugging output to <file>.
OPTION_TRIPLET(-i,interval,n)Interval between observations, in seconds (default=1).
OPTION_TRIPLET(-l,limits-file,file)Use maxfile with list of var: value pairs for resource limits.
OPTION_TRIPLET(-L,limits,string)String of the form `"var: value, var: value\' to specify resource limits.
OPTION_ITEM(`-f, --child-in-foreground')Keep the monitored process in foreground (for interactive use).
OPTION_TRIPLET(-O,with-output-files,template)Specify template for log files (default=resource-pid-<pid>).
OPTION_PAIR(--with-summary-file,file)Write resource summary to <file> (default=<template>.summary).
OPTION_PAIR(--with-time-series,file)Write resource time series to <file> (default=<template>.series).
OPTION_PAIR(`--with-opened-files',file)Write list of opened files to <file> (default=<template>.opened).
OPTION_ITEM(--without-summary-file)Do not write the summary log file.
OPTION_ITEM(--without-time-series)Do not write the time-series log file.
OPTION_ITEM(--without-opened-files)Do not write the list of opened files.
OPTION_ITEM(--with-disk-footprint)Measure working directory footprint (potentially slow).
OPTION_ITEM(--without-disk-footprint)Do not measure working directory footprint (default).
OPTION_ITEM(`-v,--version')Show version string.
OPTION_ITEM(`-h,--help')Show help text.
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
LIST_ITEM()CODE(BOLD(CCTOOLS_RESOURCE_MONITOR_HELPER)) Location of the desired helper library to wrap libc calls. If not provided, a version of the helper library is packed with the resource_monitor executable.
LIST_END

SECTION(EXIT STATUS)
The exit status of the command line provided.

SECTION(EXAMPLES)

To monitor 'sleep 10', at 2 second intervals, with output to sleep-log.summary, sleep-log.series, and sleep-log.files, and with a monitor alarm at 5 seconds:

LONGCODE_BEGIN
% resource_monitor --interval=2 -L"wall_time: 5" -o sleep-log -- sleep 10
LONGCODE_END

It can also be run automatically from makeflow, by specifying the '-M' flag:

LONGCODE_BEGIN
% makeflow -M Makeflow
LONGCODE_END

In this case, makeflow wraps every command line rule with the
monitor, and writes the resulting logs per rule in an
automatically created directory

Additionally, it can be run automatically from Work Queue:

LONGCODE_BEGIN
q = work_queue_create_monitoring(port);
work_queue_enable_monitoring(q, some-log-file);
LONGCODE_END

wraps every task with the monitor, and appends all generated
summary files into the file CODE(some-log-file).

SECTION(BUGS)

LIST_BEGIN
LIST_ITEM The monitor cannot track the children of statically linked executables.
LIST_ITEM Not all systems report major memory faults, which means IO from memory maps is computed by changes in the resident set, and therefore not very exact.
LIST_ITEM One would expect to be able to generate the information of the summary from the time-series, however they use different mechanisms, and the summary tends to be more accurate.
LIST_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER

