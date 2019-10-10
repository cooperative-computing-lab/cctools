






















# weaver(1)

## NAME
**weaver** - workflow engine for executing distributed workflows

## SYNOPSIS
****weaver [options] <weaverfile>****

## DESCRIPTION

**Weaver** is a high level interface to **makeflow**. A
**weaver** input file is written in **python**, with the
definition of functions to be applied on sets of files. **weaver**
interprets this input file and generates a workflow specification that
can be executed by **makeflow**. This allows an straightforward
implementation of different workflow execution patterns, such as
MapReduce, and AllPairs.

```


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

```

## OPTIONS

By default, running **weaver** on a <weaverfile> generates an
input file for **makeflow**, <Makeflow>, and a directory,
<_Stash>, in which intermediate files are stored.

General options:

- **-h** Give help information.
- **-W** Stop on warnings.
- **-g** Include debugging symbols in DAG.
- **-I** Do not automatically import built-ins.
- **-N** Do not normalize paths.
- **-b options** Set batch job options (cpu, memory, disk, batch, local, collect).
- **-d subsystem** Enable debugging for subsystem.
- **-o log_path** Set log path (default: stderr).
- **-O directory** Set stash output directory (default <_Stash>).


Optimization Options:


- **-a** Automatically nest abstractions.
- **-t group_size** Inline tasks based on group size.

Engine Options:


- **-x** Execute DAG using workflow engine after compiling.
- **-e arguments** Set arguments to workflow engine when executing.
- **-w wrapper ** Set workflow engine wrapper.


## EXIT STATUS

On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

**Weaver** expresses common workflow patterns succinctly. For
example, with only the following three lines of code we can express a
**map** pattern, in which we convert some images to the jpeg format:

```

convert = ParseFunction('convert {IN} {OUT}')
dataset = Glob('/usr/share/pixmaps/*.xpm')
jpgs    = Map(convert, dataset, '{basename_woext}.jpg')

```

Please refer to **cctools/doc/weaver_examples** for further information.

The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools 8.0.0 DEVELOPMENT released on 
