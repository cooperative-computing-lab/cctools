






















# weaver(1)

## NAME
**weaver** - workflow engine for executing distributed workflows

## SYNOPSIS
**weaver [options] _&lt;weaverfile&gt;_**

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

By default, running **weaver** on a _&lt;weaverfile&gt;_ generates an
input file for **makeflow**, _&lt;Makeflow&gt;_, and a directory,
_&lt;_Stash&gt;_, in which intermediate files are stored.

General options:

- **-h**<br />Give help information.
- **-W**<br />Stop on warnings.
- **-g**<br />Include debugging symbols in DAG.
- **-I**<br />Do not automatically import built-ins.
- **-N**<br />Do not normalize paths.
- **-b** _&lt;options&gt;_<br />Set batch job options (cpu, memory, disk, batch, local, collect).
- **-d** _&lt;subsystem&gt;_<br />Enable debugging for subsystem.
- **-o** _&lt;log_path&gt;_<br />Set log path (default: stderr).
- **-O** _&lt;directory&gt;_<br />Set stash output directory (default _&lt;_Stash&gt;_).


Optimization Options:


- **-a**<br />Automatically nest abstractions.
- **-t** _&lt;group_size&gt;_<br />Inline tasks based on group size.

Engine Options:


- **-x**<br />Execute DAG using workflow engine after compiling.
- **-e** _&lt;arguments&gt;_<br />Set arguments to workflow engine when executing.
- **-w** _&lt;wrapper&gt;_<br />Set workflow engine wrapper.


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

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools
