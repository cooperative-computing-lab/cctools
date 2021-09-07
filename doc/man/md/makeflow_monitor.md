






















# makeflow_monitor(1)

## NAME
**makeflow_monitor** - Makeflow log monitor

## SYNOPSIS
****makeflow_monitor [options] <makeflowlog>****

## DESCRIPTION
**makeflow_monitor** is simple **Makeflow** log monitor that displays the
progress and statistics of a workflow based on the provided <makeflowlog>.
Once started, it will continually monitor the specified <makeflowlogs> for
new events and update the progress display.

## OPTIONS

- **-h** Show this help message and exit.
- **-f format** Output format to emit.
- **-t seconds** Timeout for reading the logs.
- **-m minimum** Mininum number of tasks.
- **-S** Sort logs by progress.
- **-P** Parse dag for node information.
- **-H** Hide finished makeflows.


Currently, the only supported <format> is "text", which means
**makeflow_monitor** will display the progress of the workflows directly to
the console.

Additionally, the **-P** parameter current does not do anything.

## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES
Monitor a **Makeflow** log:
```
makeflow_monitor Makeflow.makeflowlog
```
Monitor multiple **Makeflow** logs and hide finished workflows:
```
makeflow_monitor -H */*.makeflowlog
```
Monitor multiple **Makeflow** logs under current directory and only display
currently running workflows with a minimum of 4 tasks:
```
find . -name '*.makeflowlog' | xargs makeflow_monitor -m 4 -H
```
The example above is useful for hierarchical workflows.

## COPYRIGHT
The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md) [makeflow_ec2_estimate(1)](makeflow_ec2_estimate.md)


CCTools 8.0.0 DEVELOPMENT
