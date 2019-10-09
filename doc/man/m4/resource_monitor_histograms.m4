include(manual.h)dnl
HEADER(resource_monitor_histograms)

SECTION(NAME)
BOLD(resource_monitor_histograms) - create HTML pages and graphs of resource monitor data

SECTION(SYNOPSIS)
CODE(BOLD(resource_monitor_histograms [options] -L monitor_data_file_list output_directory [workflow_name]))
CODE(BOLD(resource_monitor_histograms [options] output_directory < monitor_data_file_list  [workflow_name]))

SECTION(DESCRIPTION)

BOLD(resource_monitor_histograms) is a tool to visualize resource usage as
reported by BOLD(resource_monitor). BOLD(resource_monitor_histograms) expects
a file listing the paths of summary files (-L option or from standard
input). Results are written to BOLD(output_directory) in the form of several
webpages showing histograms and statistics per resource.

SECTION(ARGUMENTS)

SUBSECTION(Input Options)
OPTIONS_BEGIN
OPTION_PAIR(-L, monitor_data_file_list)File with one summary file path per line.
OPTIONS_END

SUBSECTION(Output Options)
OPTIONS_BEGIN
OPTION_PAIR(` output_directory')The path in which to store the visualizations. See index.html for the root of the visualization.
OPTION_ITEM(` workflow_name')Optional name to include to describe the workflow being visualized.
OPTION_PAIR(-f,str)Select which fields for the histograms. Default is "cores,memory,disk". Available fields are:
OPTIONS_END

LONGCODE_BEGIN
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
LONGCODE_END

SUBSECTION(Debugging Options)
OPTIONS_BEGIN
OPTION_TRIPLET(-d, debug, subsystem)Enable debugging for this subsystem.
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file. By default, debugging is sent to stderr (":stderr"). You may specify logs be sent to stdout (":stdout"), to the system syslog (":syslog"), or to the systemd journal (":journal").
OPTION_ITEM(`--verbose')Display runtime progress on stdout.
OPTIONS_END


SECTION(EXAMPLES)

Most common usage:

LONGCODE_BEGIN
% find my_summary_files_directory -name "*.summary" > summary_list
% resource_monitor_histograms -L summary_list my_histograms my_workflow_name
% # open my_histograms/index.html
LONGCODE_END

Splitting on categories, generating only resident memory related histograms:

LONGCODE_BEGIN
% resource_monitor_histograms -f memory -L summary_list my_histograms my_workflow_name
% # open my_histograms/index.html
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
