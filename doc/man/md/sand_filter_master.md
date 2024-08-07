






















# sand_filter_master(1)

## NAME
**sand_filter_master** - filter sequences for alignment in parallel

## SYNOPSIS
**sand_filter_master [options] sequences.cfa candidates.cand**

## DESCRIPTION

**sand_filter_master** is the first step in the SAND assembler.
It reads in a body of sequences, and uses a linear-time algorithm
to produce a list of candidate sequences to be aligned in detail
by [sand_align_master(1)](sand_align_master.md).

This program uses the Work Queue system to distributed tasks
among processors.  After starting **sand_filter_master**,
you must start a number of [work_queue_worker(1)](work_queue_worker.md) processes
on remote machines.  The workers will then connect back to the
master process and begin executing tasks.  The actual filtering
is performed by [sand_filter_kernel(1)](sand_filter_kernel.md) on each machine.

## OPTIONS


- **-p** _&lt;port&gt;_<br />Port number for queue master to listen on. (default: 9123)
- **-s** _&lt;size&gt;_<br />Number of sequences in each filtering task. (default: 1000)
- **-r** _&lt;file&gt;_<br />A meryl file of repeat mers to be filtered out.
- **-R** _&lt;n&gt;_<br />Automatically retry failed jobs up to n times. (default: 100)
- **-k** _&lt;size&gt;_<br />The k-mer size to use in candidate selection (default is 22).
- **-w** _&lt;size&gt;_<br />The minimizer window size. (default is 22).
- **-u**<br />If set, do not unlink temporary binary output files.
- **-c** _&lt;file&gt;_<br />Checkpoint filename; will be created if necessary.
- **-d** _&lt;flag&gt;_<br />Enable debugging for this subsystem.  (Try **-d all** to start.)
- **-F** _&lt;number&gt;_<br />Work Queue fast abort multiplier.     (default is 10.)
- **-Z** _&lt;file&gt;_<br />Select port at random and write it out to this file.
- **-o** _&lt;file&gt;_<br />Send debugging to this file.
- **-v**<br />Show version string
- **-h**<br />Show this help screen


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## EXAMPLES

If you begin with a FASTA formatted file of reads,
used [sand_compress_reads(1)](sand_compress_reads.md) to produce a
compressed FASTA (cfa) file.  To run filtering sequentially,
start a single [work_queue_worker(1)](work_queue_worker.md) process in the background.
Then, invoke **sand_filter_master**.

```
% sand_compress_reads mydata.fasta mydata.cfa
% work_queue_worker localhost 9123 &
% sand_filter_master mydata.cfa mydata.cand
```

To speed up the process, run more [work_queue_worker(1)](work_queue_worker.md) processes
on other machines, or use [condor_submit_workers(1)](condor_submit_workers.md) or [uge_submit_workers(1)](uge_submit_workers.md) to start hundreds of workers in your local batch system.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [SAND User Manual]("../sand.html")
- [sand_filter_master(1)](sand_filter_master.md)  [sand_filter_kernel(1)](sand_filter_kernel.md)  [sand_align_master(1)](sand_align_master.md)  [sand_align_kernel(1)](sand_align_kernel.md)  [sand_compress_reads(1)](sand_compress_reads.md)  [sand_uncompress_reads(1)](sand_uncompress_reads.md)  [work_queue_worker(1)](work_queue_worker.md)


CCTools
