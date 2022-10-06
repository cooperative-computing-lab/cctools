






















# sand_uncompress_reads(1)

## NAME
**sand_uncompress_reads** - uncompress sequence data

## SYNOPSIS
**sand_uncompress_reads [-qvh] [infile] [outfile]**

## DESCRIPTION

**sand_uncompress_reads** reads sequence data in compressed FASTA format (cfa) used by [sand_filter_master(1)](sand_filter_master.md) and [sand_align_master(1)](sand_align_master.md).
and produces output in the standard FASTA format.

If the output file is omitted, standard output is used.
if the input file is omitted, standard input is used.
After completing the compression, this program will output a summary
line of sequences and bytes compressed.

## OPTIONS


- **-q**<br />Quiet mode: suppress summary line.
- **-h**<br />Display version information.
- **-v**<br />Show help text.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To uncompress **mydata.cfa** into **mydata.fasta**:

```
% sand_uncompress_reads mydata.cfa mydata.fa
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [SAND User Manual]("../sand.html")
- [sand_filter_master(1)](sand_filter_master.md)  [sand_filter_kernel(1)](sand_filter_kernel.md)  [sand_align_master(1)](sand_align_master.md)  [sand_align_kernel(1)](sand_align_kernel.md)  [sand_compress_reads(1)](sand_compress_reads.md)  [sand_uncompress_reads(1)](sand_uncompress_reads.md)  [work_queue_worker(1)](work_queue_worker.md)


CCTools
