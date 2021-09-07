






















# sand_compress_reads(1)

## NAME
**sand_compress_reads** - compress sequence data

## SYNOPSIS
**sand_compress_reads [-qcivh] [infile] [outfile]**

## DESCRIPTION

**sand_compress_reads** reads sequence data in standard FASTA format,
and produces output in the compressed FASTA (cfa) format used by
the [sand_filter_master(1)](sand_filter_master.md) and [sand_align_master(1)](sand_align_master.md).

If the output file is omitted, standard output is used.
if the input file is omitted, standard input is used.
After completing the compression, this program will output a summary
line of sequences and bytes compressed.

## OPTIONS


- **-q**<br />Quiet mode: suppress summary line.
- **-c**<br />Remove Celera read ids if data came from the Celera gatekeeper.
- **-i**<br />Remove Celera read ids but leave internal ids if the data came from the Celera gatekeeper.
- **-h**<br />Display version information.
- **-v**<br />Show help text.


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

To compress **mydata.fasta** into **mydata.cfa**:

```
% sand_compress_reads mydata.fasta mydata.cfa
```

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [SAND User Manual]("../sand.html")
- [sand_filter_master(1)](sand_filter_master.md)  [sand_filter_kernel(1)](sand_filter_kernel.md)  [sand_align_master(1)](sand_align_master.md)  [sand_align_kernel(1)](sand_align_kernel.md)  [sand_compress_reads(1)](sand_compress_reads.md)  [sand_uncompress_reads(1)](sand_uncompress_reads.md)  [work_queue_worker(1)](work_queue_worker.md)


CCTools 8.0.0 DEVELOPMENT
