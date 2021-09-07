






















# resource_monitor_histograms(1)

## NAME
**resource_monitor_histograms** - create HTML pages and graphs of resource monitor data

## SYNOPSIS
****resource_monitor_histograms [options] -L monitor_data_file_list output_directory [workflow_name]****
****resource_monitor_histograms [options] output_directory < monitor_data_file_list  [workflow_name]****

## DESCRIPTION

**resource_monitor_histograms** is a tool to visualize resource usage as
reported by **resource_monitor**. **resource_monitor_histograms** expects
a file listing the paths of summary files (-L option or from standard
input). Results are written to **output_directory** in the form of several
webpages showing histograms and statistics per resource.

## ARGUMENTS

### Input Options

- **-L monitor_data_file_list** File with one summary file path per line.


### Output Options

- ** output_directory ** The path in which to store the visualizations. See index.html for the root of the visualization.
- ** workflow_name** Optional name to include to describe the workflow being visualized.
- **-f str** Select which fields for the histograms. Default is "cores,memory,disk". Available fields are:


```
bandwidth
bytes_read
bytes_received
bytes_send
bytes_written
cores
cpu_time
disk
max_concurrent_processes
memory
swap_memory
total_files
total_processes
virtual_memory
wall_time
```

### Debugging Options

- **-d --debug <subsystem>** Enable debugging for this subsystem.
- **-o --debug-file <file>** Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs to be sent to stdout (":stdout") instead.
- **--verbose** Display runtime progress on stdout.



## EXAMPLES

Most common usage:

```
% find my_summary_files_directory -name "*.summary" > summary_list
% resource_monitor_histograms -L summary_list my_histograms my_workflow_name
% # open my_histograms/index.html
```

Splitting on categories, generating only resident memory related histograms:

```
% resource_monitor_histograms -f memory -L summary_list my_histograms my_workflow_name
% # open my_histograms/index.html
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools 7.3.2 FINAL
