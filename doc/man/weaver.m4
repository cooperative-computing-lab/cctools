include(manual.h)dnl
HEADER(weaver)

SECTION(NAME)
BOLD(weaver) - workflow engine for executing distributed workflows

SECTION(SYNOPSIS)
CODE(BOLD(weaver [options] PARAM(weaverfile)))

SECTION(DESCRIPTION)

BOLD(Weaver) is a high level interface to BOLD(makeflow). A
BOLD(weaver) input file is written in BOLD(python), with the
definition of functions to be applied on sets of files. BOLD(weaver)
interprets this input file and generates a workflow specification that
can be executed by BOLD(makeflow). This allows an straightforward
implementation of different workflow execution patterns, such as
MapReduce, and AllPairs.

LONGCODE_BEGIN


				      /--------\
				    +-+ Python |
				    | \---+----/
+---------------------------------+ |     | Generate DAG
|	      Weaver		  +-+     v
+---------------------------------+   /-------\
|	     Makeflow		  +---+  DAG  |
+--------+-----------+-----+------+   \-------/
| Condor | WorkQueue | SGE | Unix +-+     | Dispatch Jobs
+--------+-----------+-----+------+ |     v
				    | /-------\
				    +-+ Jobs  |
				      \-------/

LONGCODE_END

SECTION(OPTIONS)

By default, running CODE(weaver) on a PARAM(weaverfile) generates an
input file for CODE(makeflow), PARAM(Makeflow), and a directory,
PARAM(_Stash), in which intermediate files are stored.

General options:
OPTIONS_BEGIN
OPTION_ITEM(`-h')Give help information.
OPTION_ITEM(`-W')Stop on warnings.
OPTION_ITEM(`-g')Include debugging symbols in DAG.
OPTION_ITEM(`-I')Do not automatically import built-ins.
OPTION_ITEM(`-N')Do not normalize paths.
OPTION_PAIR(`-b', options)Set batch job options (cpu, memory, disk, batch, local, collect).
OPTION_PAIR(`-d', subsystem)Enable debugging for subsystem.
OPTION_PAIR(`-o', log_path)Set log path (default: stderr).
OPTION_PAIR(`-O', directory)Set stash output directory (default PARAM(_Stash)).
OPTIONS_END

Optimization Options:
OPTIONS_BEGIN
OPTIONS_END
OPTION_ITEM(`-a')Automatically nest abstractions.
OPTION_PAIR(`-t', group_size)Inline tasks based on group size.

Engine Options:

OPTIONS_BEGIN
OPTION_ITEM(`-x')Execute DAG using workflow engine after compiling.
OPTION_PAIR(`-e', arguments)Set arguments to workflow engine when executing.
OPTION_PAIR(`-w' wrapper)Set workflow engine wrapper.
OPTIONS_END

SECTION(EXIT STATUS)

On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

BOLD(Weaver) expresses common workflow patterns succinctly. For
example, with only the following three lines of code we can express a
BOLD(map) pattern, in which we convert some images to the jpeg format:

LONGCODE_BEGIN

convert = ParseFunction('convert {IN} {OUT}')
dataset = Glob('/usr/share/pixmaps/*.xpm')
jpgs    = Map(convert, dataset, '{basename_woext}.jpg')

LONGCODE_END

Please refer to CODE(cctools/doc/weaver_examples) for further information.

COPYRIGHT_BOILERPLATE

FOOTER
