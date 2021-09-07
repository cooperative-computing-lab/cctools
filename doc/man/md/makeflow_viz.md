






















# makeflow_viz(1)

## NAME
**makeflow_viz** - visualization of Makeflow workflows

## SYNOPSIS
**makeflow_viz [options] _&lt;dagfile&gt;_**

## DESCRIPTION

**makeflow_viz** is a collection of tools to graphically display workflows. This includes DOT and PPM.

## OPTIONS
### Commands

- **-v**,**--version**<br />Show version string.
- **-h**,**--help**<br />Show help message.
- **-D**,**--display=_&lt;opt&gt;_**<br /> Translate the makeflow to the desired visualization format:
    dot      DOT file format for precise graph drawing.
    ppm      PPM file format for rapid iconic display.
    cyto     Cytoscape format for browsing and customization.
    dax      DAX format for use by the Pegasus workflow manager.
    json     JSON representation of the DAG.
- **-- **<br />Options for dot output:
- **--dot-merge-similar**<br />Condense similar boxes
- **--dot-proportional**<br />Change the size of the boxes proportional to file size
- **--dot-no-labels**<br />Show only shapes with no text labels.
- **--dot-details**<br />Display a more detailed graph including an operating sandbox for each task.
- **--dot-task-id**<br />Set task label to ID number instead of command.
- **--dot-graph-attr**<br />Set graph attributes.
- **--dot-node-attr**<br />Set node attributes.
- **--dot-edge-attr**<br />Set edge attributes.
- **--dot-task-attr**<br />Set task attributes.
- **--dot-file-attr**<br />Set file attributes.
- **-- **<br />The following options for ppm generation are mutually exclusive:
- **--ppm-highlight-row=_&lt;row&gt;_**<br />Highlight row _&lt;row&gt;_ in completion grap
- **--ppm-highlight-row=_&lt;row&gt;_**<br />Highlight row _&lt;row&gt;_ in completion grap
- **--ppm-highlight-file=_&lt;file&gt;_**<br />Highlight node that creates file _&lt;file&gt;_ in completion graph
- **--ppm-highlight-file=_&lt;file&gt;_**<br />Highlight node that creates file _&lt;file&gt;_ in completion graph
- **--ppm-highlight-executable=_&lt;exe&gt;_**<br />Highlight executable _&lt;exe&gt;_ in completion grap
- **--ppm-highlight-executable=_&lt;exe&gt;_**<br />Highlight executable _&lt;exe&gt;_ in completion grap
- **--ppm-show-levels**<br />Display different levels of depth in completion graph
- **-- **<br />Options for JSON output:
- **--json**<br />Use JSON format for the workflow specification.
- **--jx**<br />Use JX format for the workflow specification.
- **--jx-args=_&lt;_&lt;file&gt;_&gt;_**<br />Evaluate the JX input with keys and values in file defined as variables.
- **--jx-args=_&lt;_&lt;file&gt;_&gt;_**<br />Evaluate the JX input with keys and values in file defined as variables.
- **--jx-define=_&lt;_&lt;var&gt;_=_&lt;expr&gt;_&gt;_**<br />Set the JX variable VAR to the JX expression EXPR.
- **--jx-define=_&lt;_&lt;var&gt;_=_&lt;expr&gt;_&gt;_**<br />Set the JX variable VAR to the JX expression EXPR.




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
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools 7.3.2 FINAL
