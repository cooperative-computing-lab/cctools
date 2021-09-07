






















# sand_filter_kernel(1)

## NAME
**sand_filter_kernel** - filter read sequences sequentially

## SYNOPSIS
**sand_filter_kernel [options] _&lt;sequence file&gt;_ [second sequence file]**

## DESCRIPTION

**sand_filter_kernel** filters a list of genomic sequences,
and produces a list of candidate pairs for more detailed alignment.
It is not normally called by the user, but is invoked by
[sand_filter_master(1)](sand_filter_master.md) for each sequential step of a distributed
alignment workload.

If one sequence file is given, **sand_filter_kernel** will look for
similarities between all sequences in that file.  If given two files,
it will look for similarities between sequences in the first file and
the second file.  The output is a list of candidate pairs, listing the
name of the candidate sequences and a starting position for alignment.

## OPTIONS


- **-s** _&lt;size&gt;_<br />Size of "rectangle" for filtering. You can determine
the size dynamically by passing in d rather than a number.
- **-r** _&lt;file&gt;_<br />A meryl file of repeat mers to be ignored.
- **-k** _&lt;size&gt;_<br />The k-mer size to use in candidate selection (default is 22).
- **-w** _&lt;number&gt;_<br />The minimizer window size to use in candidate selection (default is 22).
- **-o** _&lt;filename&gt;_<br />The output file. Default is stdout.
- **-d** _&lt;subsystem&gt;_<br />Enable debug messages for this subsystem.  Try **-d all** to start.
- **-v**<br />Show version string.
- **-h**<br />Show help screen.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

Users do not normally invoke **sand_filter_kernel** directly.  Instead, options such as the k-mer size, minimizer window, and repeat file may be specified by the same arguments to [sand_filter_master(1)](sand_filter_master.md) instead.  For example, to run a filter with a k-mer size of 20, window size of 24, and repeat file of **mydata.repeats**:

```
% sand_filter_master -k 20 -w 24 -r mydata.repeats mydata.cfa mydata.cand
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [SAND User Manual]("../sand.html")
- [sand_filter_master(1)](sand_filter_master.md)  [sand_filter_kernel(1)](sand_filter_kernel.md)  [sand_align_master(1)](sand_align_master.md)  [sand_align_kernel(1)](sand_align_kernel.md)  [sand_compress_reads(1)](sand_compress_reads.md)  [sand_uncompress_reads(1)](sand_uncompress_reads.md)  [work_queue_worker(1)](work_queue_worker.md)


CCTools 7.3.2 FINAL
