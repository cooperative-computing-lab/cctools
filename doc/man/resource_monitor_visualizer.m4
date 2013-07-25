include(manual.h)dnl
HEADER(resource_monitor_visualizer)

SECTION(NAME)
BOLD(resource_monitor_visualizer.py) - create HTML pages and graphs of resource monitor data

SECTION(SYNOPSIS)
CODE(BOLD(resource_monitor_visualizer.py data_path destination_path workflow_name))

SECTION(DESCRIPTION)

BOLD(resource_monitor_visualizer) is a tool to visualize resource usage of the
given workflow. The visualizer creates a series of web pages and graphs to
explore resource usage. Histograms are created for each group and resource.
Currently, groups are determined by command name. Additionally, if time series
data is recorded graphs are created to show the aggregate resource usage over
time.

SECTION(ARGUMENTS)
OPTIONS_BEGIN
OPTION_ITEM(` data_path')The path to the data recorded by the resource_monitor.
OPTION_ITEM(` destination_path')The path in which to store the visualization.
OPTION_ITEM(` workflow_name')The name of the workflow being visualized.
OPTIONS_END

SECTION(EXAMPLES)

To visualize a collection of resource data:
LONGCODE_BEGIN
% resource_monitor.py /path/to/logs /home/guest/sites my_workflow
LONGCODE_END

SECTION(BUGS)

LIST_BEGIN
LIST_ITEM x-axis labels can overlap when using older versions of gnuplot.
LIST_ITEM Aggregate time series plots are noisy.
LIST_ITEM The layout is optimized for large displays and can become cluttered on smaller monitors.
LIST_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER

