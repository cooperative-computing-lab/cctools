include(manual.h)dnl
HEADER(makeflow_viz)

SECTION(NAME)
BOLD(makeflow_viz) - visualization of Makeflow workflows

SECTION(SYNOPSIS)
CODE(makeflow_viz [options] PARAM(dagfile))

SECTION(DESCRIPTION)

BOLD(makeflow_viz) is a collection of tools to graphically display workflows. This includes DOT and PPM.

SECTION(OPTIONS)
SUBSECTION(Commands)
OPTIONS_BEGIN
OPTION_FLAG(v,version)Show version string.
OPTION_FLAG(h,help)Show help message.
OPTION_ARG(D, display, opt) Translate the makeflow to the desired visualization format:
    dot      DOT file format for precise graph drawing.
    ppm      PPM file format for rapid iconic display.
    cyto     Cytoscape format for browsing and customization.
    dax      DAX format for use by the Pegasus workflow manager.
    json     JSON representation of the DAG.
OPTION_FLAG_LONG(` ')Options for dot output:
OPTION_FLAG_LONG(dot-merge-similar)Condense similar boxes
OPTION_FLAG_LONG(dot-proportional)Change the size of the boxes proportional to file size
OPTION_FLAG_LONG(dot-no-labels)Show only shapes with no text labels.
OPTION_FLAG_LONG(dot-details)Display a more detailed graph including an operating sandbox for each task.
OPTION_FLAG_LONG(dot-task-id)Set task label to ID number instead of command.
OPTION_FLAG_LONG(dot-graph-attr)Set graph attributes.
OPTION_FLAG_LONG(dot-node-attr)Set node attributes.
OPTION_FLAG_LONG(dot-edge-attr)Set edge attributes.
OPTION_FLAG_LONG(dot-task-attr)Set task attributes.
OPTION_FLAG_LONG(dot-file-attr)Set file attributes.
OPTION_FLAG_LONG(` ')The following options for ppm generation are mutually exclusive:
OPTION_ARG_LONG(ppm-highlight-row, row)Highlight row PARAM(row) in completion grap
OPTION_ARG_LONG(ppm-highlight-row, row)Highlight row PARAM(row) in completion grap
OPTION_ARG_LONG(ppm-highlight-file,file)Highlight node that creates file PARAM(file) in completion graph
OPTION_ARG_LONG(ppm-highlight-file,file)Highlight node that creates file PARAM(file) in completion graph
OPTION_ARG_LONG(ppm-highlight-executable,exe)Highlight executable PARAM(exe) in completion grap
OPTION_ARG_LONG(ppm-highlight-executable,exe)Highlight executable PARAM(exe) in completion grap
OPTION_FLAG_LONG(ppm-show-levels)Display different levels of depth in completion graph
OPTION_FLAG_LONG(` ')Options for JSON output:
OPTION_FLAG_LONG(json)Use JSON format for the workflow specification.
OPTION_FLAG_LONG(jx)Use JX format for the workflow specification.
OPTION_ARG_LONG(jx-args, PARAM(file))Evaluate the JX input with keys and values in file defined as variables.
OPTION_ARG_LONG(jx-args, PARAM(file))Evaluate the JX input with keys and values in file defined as variables.
OPTION_ARG_LONG(jx-define, PARAM(var)=PARAM(expr))Set the JX variable VAR to the JX expression EXPR.
OPTION_ARG_LONG(jx-define, PARAM(var)=PARAM(expr))Set the JX variable VAR to the JX expression EXPR.
OPTIONS_END



SECTION(EXAMPLES)

To produce a DOT representation of the workflow
LONGCODE_BEGIN
makeflow_viz -D dot Makeflow
LONGCODE_END

To produce a cytoscape representation of the workflow
LONGCODE_BEGIN
makeflow_viz -D cyto Makeflow
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
