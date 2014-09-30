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
can be executed by BOLD(makeflow). This allows an straightforward implementation of different workflow execution patterns, such as MapReduce, and AllPairs.

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

LONGCODE_BEGIN

      Input	    Compiler	     Output

    +--------+	 /-------------\   +---------+
    |	     |   |	       |   |	     |
    | Python |-->|   Weaver    |-->| Sandbox |
    | Script |	 |	       |   |   {s}   |
    |	 {d} |	 |	       |   |	     |
    +--------+	 \-------------/   +---------+
				        |
		       +----------------+
		       |
	 +-------------+---------------+
	 |	       |	       |
	 v	       v               v
    +---------+ +-------------+ +------------+	  
    |	      |	|	      | |	     |
    | Scripts | | Executables | | Input Data |
    |	  {d} | |         {d} | |        {d} |
    |	      |	|             | |	     |
    +---------+ +-------------+ +------------+

LONGCODE_END


SECTION(OPTIONS)

When CODE(weaver) is ran without arguments...

SUBSECTION(Commands)
OPTIONS_BEGIN
OPTIONS_END

SECTION(ENVIRONMENT VARIABLES)

The following environment variables will affect the execution of your
BOLD(Weaver):
SUBSECTION(....)

SECTION(EXIT STATUS)

On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Map example.

LONGCODE_BEGIN

from weaver.function import SimpleFunction
from glob import glob

ImgToPNG = SimpleFunction('convert', '', '.png')
ImgToJPG = SimpleFunction('convert', '', '.jpg')

xpms = glob('/usr/share/pixmaps/*.xpm')
pngs = Map(ImgToPNG, xpms)
jpgs = Map(ImgToJPG, pngs)

LONGCODE_END

LONGCODE_BEGIN

 from glob import glob

 def wc_mapper(key, value):
     for w in value.split():
         print '%s\t%d' % (w, 1)

 def wc_reducer(key, values):
     value = sum(int(v) for v in values)
     print '%s\t%d' % (key, value)

 MapReduce(  mapper  = wc_mapper,
             reducer = wc_reducer,
             input   = glob('weaver/*.py'),
             output  = 'wc.txt') 

LONGCODE_END

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_WEAVER

FOOTER
