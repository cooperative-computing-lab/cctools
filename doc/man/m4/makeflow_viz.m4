include(manual.h)dnl
HEADER(makeflow_viz)

SECTION(NAME)
BOLD(makeflow_viz) - visualization of Makeflow workflows

SECTION(SYNOPSIS)
CODE(BOLD(makeflow_viz [options] PARAM(dagfile)))

SECTION(DESCRIPTION)

BOLD(makeflow_viz) is a collection of tools to graphically display workflows. This includes DOT and PPM.

SECTION(OPTIONS)
SUBSECTION(Commands)
OPTIONS_BEGIN
OPTION_ITEM(`-v, --version')Show version string.
OPTION_ITEM(`-h, --help')Show help message.
OPTION_TRIPLET(-D, display, opt) Translate the makeflow to the desired visualization format:
    dot      DOT file format for precise graph drawing.
    ppm      PPM file format for rapid iconic display.
    cyto     Cytoscape format for browsing and customization.
    dax      DAX format for use by the Pegasus workflow manager.
    json     JSON representation of the DAG.
OPTION_ITEM(` ')Options for dot output:
OPTION_ITEM(`--dot-merge-similar')Condense similar boxes
OPTION_ITEM(`--dot-proportional')Change the size of the boxes proportional to file size
OPTION_ITEM(`--dot-no-labels')Show only shapes with no text labels.
OPTION_ITEM(`--dot-details')Display a more detailed graph including an operating sandbox for each task.
OPTION_ITEM(`--dot-task-id')Set task label to ID number instead of command.
OPTION_ITEM(`--dot-graph-attr')Set graph attributes.
OPTION_ITEM(`--dot-node-attr')Set node attributes.
OPTION_ITEM(`--dot-edge-attr')Set edge attributes.
OPTION_ITEM(`--dot-task-attr')Set task attributes.
OPTION_ITEM(`--dot-file-attr')Set file attributes.
OPTION_ITEM(` ')The following options for ppm generation are mutually exclusive:
OPTION_PAIR(--ppm-highlight-row, row)Highlight row <row> in completion grap
OPTION_PAIR(--ppm-highlight-file,file)Highlight node that creates file <file> in completion graph
OPTION_PAIR(--ppm-highlight-executable,exe)Highlight executable <exe> in completion grap
OPTION_ITEM(`--ppm-show-levels')Display different levels of depth in completion graph
OPTION_ITEM(` ')Options for JSON output:
OPTION_ITEM(`--json')Use JSON format for the workflow specification.
OPTION_ITEM(`--jx')Use JX format for the workflow specification.
OPTION_PAIR(--jx-args, <file>)Evaluate the JX input with keys and values in file defined as variables.
OPTION_PAIR(--jx-define, <var>=<expr>)Set the JX variable VAR to the JX expression EXPR.
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
