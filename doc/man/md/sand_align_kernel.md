






















# sand_align_kernel(1)

## NAME
**sand_align_kernel** - align candidate sequences sequentially

## SYNOPSIS
**sand_align_kernel [options] [input file]**

## DESCRIPTION

**sand_align_kernel** aligns candidate sequences sequentially.
It is not normally called by the user, but is invoked by
[sand_align_master(1)](sand_align_master.md) for each sequential step of a distributed
alignment workload.  The options to **sand_align_kernel** control
the type of alignment (Smith-Waterman, Prefix-Suffix, Banded, etc)
and the quality threshhold for reporting alignments.  These options
are typically passed in by giving the **-e** option to **sand_align_master**.

**sand_align_kernel** reads a list of sequences
from the given input file, or from standard input if none is given.
The sequences are in the compressed fasta format produced
by [sand_compress_reads(1)](sand_compress_reads.md).  The first sequence in the input
is compared against all following sequences until a separator line.
Following the separator, the next sequence is compared against
all following sequences until the following separator, and so on.

## OPTIONS


- **-a** _&lt;sw|ps|banded&gt;_<br />Specify the type of alignment: sw (Smith-Waterman), ps (Prefix-Suffix), or banded.  If not specified, default is banded.
- **-o** _&lt;ovl|ovl_new|align|matrix&gt;_<br />Specify how each alignment should be output: ovl (Celera V5, V6 OVL format), ovl_new (Celera V7 overlap format), align (display the sequences and alignment graphically) or matrix (display the dynamic programming matrix).  [sand_align_master(1)](sand_align_master.md) expects the ovl output format, which is the default.  The other formats are useful for debugging.
- **-m** _&lt;length&gt;_<br />Minimum aligment length (default: 0).
- **-q** _&lt;quality&gt;_<br />Minimum match quality (default: 1.00)
- **-x**<br />Delete input file after completion.
- **-d** _&lt;subsystem&gt;_<br />Enable debugging for this subsystem.  (Try **-d all** to start.
- **-v**<br />Show program version.
- **-h**<br />Display this message.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Users do not normally invoke sand_align_kernel directly.  Instead, pass arguments by using the **-e** option to [sand_align_master(1)](sand_align_master.md).  For example, to specify a minimum alignment length of 5 and a minimum quality of 0.25:

```
% sand_align_master sand_align_kernel -e "-m 5 -q 0.25" mydata.cand mydata.cfa mydata.ovl
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [SAND User Manual]("../sand.html")
- [sand_filter_master(1)](sand_filter_master.md)  [sand_filter_kernel(1)](sand_filter_kernel.md)  [sand_align_master(1)](sand_align_master.md)  [sand_align_kernel(1)](sand_align_kernel.md)  [sand_compress_reads(1)](sand_compress_reads.md)  [sand_uncompress_reads(1)](sand_uncompress_reads.md)  [work_queue_worker(1)](work_queue_worker.md)


CCTools
