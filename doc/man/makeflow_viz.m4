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
OPTION_TRIPLET(-D, display, opt)Display the Makefile as a Dot graph or a PPM completion graph. <opt> is one of:
   dot      Standard Dot graph
   file     Display the file as interpreted by Makeflow
   ppm      Display a completion graph in PPM format
OPTION_ITEM(`--dot-merge-similar')Condense similar boxes
OPTION_ITEM(`--dot-proportional')Change the size of the boxes proportional to file size
OPTION_ITEM(` ')The following options for ppm generation are mutually exclusive:
OPTION_PAIR(--ppm-highlight-row, row)Highlight row <row> in completion grap
OPTION_PAIR(--ppm-highlight-file,file)Highlight node that creates file <file> in completion graph
OPTION_PAIR(--ppm-highlight-executable,exe)Highlight executable <exe> in completion grap
OPTION_ITEM(`--ppm-show-levels')Display different levels of depth in completion graph
OPTION_ITEM(`-e, --export-as-dax')Export the DAG in DAX format. (Pegasus)
OPTION_ITEM(`-v, --version')Show version string.
OPTIONS_END

SECTION(EXAMPLES)

To produce a DOT representation of the workflow
LONGCODE_BEGIN
makeflow_viz -D dot Makeflow
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER
