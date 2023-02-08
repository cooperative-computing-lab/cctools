






















# makeflow_analyze(1)

## NAME
**makeflow_analyze** - analysis of Makeflow workflows

## SYNOPSIS
**makeflow_analyze [options] _&lt;dagfile&gt;_**

## DESCRIPTION

**makeflow_analyze** is a collection of tools to provide insight into the structure of workflows. This includes syntax analysis and dag statistics.

## OPTIONS
### Commands

- **-b**,**--bundle-dir=_&lt;directory&gt;_**<br />Create portable bundle of workflow.
- **-h**,**--help**<br />Show this help screen.
- **-I**,**--show-input**<br />Show input files.
- **-k**,**--syntax-check**<br />Syntax check.
- **-O**,**--show-output**<br />Show output files.
- **-v**,**--version**<br />Show version string.


## EXAMPLES

Analyze the syntax of a workflow:
```
makeflow_analyze -k Makeflow
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
