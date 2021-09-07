






















# makeflow_viz(1)

## NAME
**makeflow_viz** - visualization of Makeflow workflows

## SYNOPSIS
****makeflow_viz [options] <dagfile>****

## DESCRIPTION

**makeflow_viz** is a collection of tools to graphically display workflows. This includes DOT and PPM.

## OPTIONS
### Commands

- **-v, --version** Show version string.
- **-h, --help** Show help message.
- **-D --display <opt>**  Translate the makeflow to the desired visualization format:
    dot      DOT file format for precise graph drawing.
    ppm      PPM file format for rapid iconic display.
    cyto     Cytoscape format for browsing and customization.
    dax      DAX format for use by the Pegasus workflow manager.
    json     JSON representation of the DAG.
- ** ** Options for dot output:
- **--dot-merge-similar** Condense similar boxes
- **--dot-proportional** Change the size of the boxes proportional to file size
- **--dot-no-labels** Show only shapes with no text labels.
- **--dot-details** Display a more detailed graph including an operating sandbox for each task.
- **--dot-task-id** Set task label to ID number instead of command.
- **--dot-graph-attr** Set graph attributes.
- **--dot-node-attr** Set node attributes.
- **--dot-edge-attr** Set edge attributes.
- **--dot-task-attr** Set task attributes.
- **--dot-file-attr** Set file attributes.
- ** ** The following options for ppm generation are mutually exclusive:
- **--ppm-highlight-row row** Highlight row <row> in completion grap
- **--ppm-highlight-file file** Highlight node that creates file <file> in completion graph
- **--ppm-highlight-executable exe** Highlight executable <exe> in completion grap
- **--ppm-show-levels** Display different levels of depth in completion graph
- ** ** Options for JSON output:
- **--json** Use JSON format for the workflow specification.
- **--jx** Use JX format for the workflow specification.
- **--jx-args <file>** Evaluate the JX input with keys and values in file defined as variables.
- **--jx-define <var>=<expr>** Set the JX variable VAR to the JX expression EXPR.




## EXAMPLES

To produce a DOT representation of the workflow
```
makeflow_viz -D dot Makeflow
```

To produce a cytoscape representation of the workflow
```
makeflow_viz -D cyto Makeflow
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md) [makeflow_ec2_estimate(1)](makeflow_ec2_estimate.md)


CCTools 8.0.0 DEVELOPMENT
