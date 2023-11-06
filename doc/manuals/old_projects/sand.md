# SAND User's Manual

**Last edited: January 2011**

SAND is Copyright (C) 2022 The University of Notre Dame.  
All rights reserved.  
This software is distributed under the GNU General Public License.  
See the file COPYING for details.

## Overview

SAND is a set of modules for accelerating genome assembly and other
bioinformatics tasks. Using the [Work
Queue](http://ccl.cse.nd.edu/software/workqueue/) framework, SAND can
distribute computational tasks to hundreds of nodes harnessed from clusters,
clouds, grids, or just the idle machines you have in your office. SAND can be
used as a drop-in replacement for the conventional overlapper in the Celera
Assembler, or can be used as a standalone tool by advanced users.

SAND is part of the [Cooperating Computing
Tools](http://ccl.cse.nd.edu/software). You can download the CCTools from
[this web page](http://ccl.cse.nd.edu/software/download), follow the
[installation instructions](../install/index.md), and you are ready to go.

## Using SAND with the Celera Assembler

If you are already using the Celera Assembler version 5.4, you can easily
switch to using SAND to accelerate assemblies by using our modified
`sand_runCA_5.4` script. If you are using the newer Celera Assembler version
6.1, you can switch to SAND by using the ` sand_runCA_6.1 ` script. If you are
using Celera Assembler version 7.0, you can switch to SAND by using the `
sand_runCA_7.0 ` script. We assume that you have installed SAND according to
the instructions above and addeed it to your `PATH`.

  1. Copy the script `sand_runCA_X.X` into the same directory where you have the normal `runCA` installed. 
  2. Set `ovlOverlapper=sand` to your `spec` file. 
  3. For Celera 7.0, add `sandAlignFlags=-e "-o ovl_new"` to your `spec` file. This tells the alignment program to output new Celera 7.0 style overlaps. 
  4. Run `sand_runCA_X.X` just like you normally use `runCA` 
You will see the assembly start as normal. When the overlapping stage begins,
you will see output like this: `Total | Workers | Tasks Avg | Candidates Time
| Idle Busy | Submit Idle Run Done Time | Found 0 | 0 0 | 0 0 0 0 nan | 0 5 |
0 0 | 0 0 0 0 nan | 0 10 | 0 0 | 0 0 0 0 nan | 0 15 | 0 0 | 0 0 0 0 nan | 0
... ` SAND is now waiting for you to start `work_queue_worker` processes. Each
worker that you start will connect back to the master process nd perform small
pieces of the work at a time. In general, the more machines that you can
harness, the faster the work will go.

For testing SAND, just open a new console window and start a single worker,
specifying the hostname where the master runs and the port number it is
listening on. (This can be changed with the sandPort option in the CA spec
file.) For example: `% work_queue_worker master.somewhere.edu 9123` With one
worker, you will see some change in the output, like this: `Total | Workers |
Tasks Avg | Candidates Time | Idle Busy | Submit Idle Run Done Time | Found 0
| 0 0 | 0 0 0 0 nan | 0 5 | 0 1 | 100 83 1 16 0.32 | 1858 10 | 0 1 | 100 69 1
30 0.33 | 3649 15 | 0 1 | 100 55 1 44 0.34 | 5464 ` For a very small assembly,
a single worker might be sufficient. However, to run a really large assembly
at scale, you will need to run as many workers as possible. A simple (but
tiresome) way of doing so is to `ssh` into lots of machines and manually run
`work_queue_worker` as above. But, if you have access to a batch system like
[Condor](http://www.cs.wisc.edu/condor) or
[SGE](http://www.sun.com/software/sge), you can use them to start many workers
with a single submit command.

We have provided some simple scripts to make this easy. For example, to submit
10 workers to your local Condor pool: `% condor_submit_workers
master.somewhere.edu 9123 10 Submitting job(s).......... Logging submit
event(s).......... 10 job(s) submitted to cluster 298. ` Or, to submit 10
worker processes to your SGE cluster: `% sge_submit_workers
master.somewhere.edu 9123 10 Your job 1054781 ("worker.sh") has been submitted
Your job 1054782 ("worker.sh") has been submitted Your job 1054783
("worker.sh") has been submitted ... ` Note that `condor_submit_workers` and
`sge_submit_workers` are simple shell scripts, so you can edit them directly
if you would like to change batch options or other details.

Once the workers begin running, the SAND modules can dispatch tasks to each
one very quickly. It's ok if a machine running a worker crashes or is turned
off; the work will be silently sent elsewhere to run.

When the SAND module's master process completes, your workers will still be
available, so you can either run another master with the same workers, remove
them from the batch system, or wait for them to expire. If you do nothing for
15 minutes, they will automatically exit.

## SAND in More Detail

This section explains the two SAND modules in more detail, if you would like
to tune the performance or use them independently of Celera. We assume that
you begin with a file of reads in FASTA format. To use the SAND modules, you
must first generate repeats and compress the data. (If you don't have data
handy, download `small.cfa` and `small.repeats` data from the [SAND
Webpage](http://ccl.cse.nd.edu/software/sand).) To generate repeats from a
FASTA file, use the `meryl` tool from the Celera Assembler: `% meryl -B -m 24
-C -L 100 -v -o small.meryl -s small.fa % meryl -Dt -s small.meryl -n 100 >
small.repeats ` Then use `sand_compress_reads` to compress the sequence data
into a compressed FASTA (.cfa) file: `% sand_compress_reads small.fa
small.cfa` The filtering step will read in the compressed sequence data
(`small.cfa`) and quickly produce a list of candidate sequences (`small.cand`)
for the following step to consider in detail. Start the filtering step as
follows: `% sand_filter_master -r small.repeats small.cfa small.cand` While
the filtering step runs, it will print some statistics to the console, showing
the number of workers available, tasks running, and so forth: `Total | Workers
| Tasks Avg | Candidates Time | Idle Busy | Submit Idle Run Done Time | Found
0 | 0 0 | 0 0 0 0 nan | 0 5 | 0 14 | 158 14 14 130 0.39 | 16452 10 | 0 15 |
356 15 15 326 0.38 | 42382 15 | 0 15 | 549 15 15 519 0.38 | 69055 20 | 0 15 |
744 15 15 714 0.38 | 96298 25 | 0 15 | 942 15 15 912 0.38 | 124284 ` The
alignment step will take the list of candidates generated in the previous step
(`small.cand`), the compressed sequences (`small.cfa`) and produce a listing
of how and where the sequences overlap (`small.ovl`). For example: `%
sand_align_master sand_align_kernel -e "-q 0.04 -m 40" small.cand small.cfa
small.ovl` The options `-q 0.04 -m 40` passed to `sand_align_kernel` indicate
a minimum alignment quality of 0.04 and a minimum alignment length of 40
bases. Again, a progress table will be printed to standard out: `Total |
Workers | Tasks Avg | K-Cand K-Seqs | Total Time | Idle Busy | Submit Idle Run
Done Time | Loaded Loaded | Speedup 0 | 0 0 | 0 0 0 0 0.00 | 0 0 | 0.00 8 | 0
48 | 100 52 48 0 0.00 | 1000 284 | 0.00 10 | 0 86 | 100 13 86 1 7.07 | 1000
284 | 0.71 36 | 1 83 | 181 14 83 2 19.47 | 1810 413 | 1.08 179 | 1 83 | 259 92
83 3 22.51 | 2590 1499 | 0.38 186 | 2 80 | 259 15 80 85 28.54 | 2590 1499 |
13.04 199 | 2 80 | 334 90 80 86 29.96 | 3340 1499 | 12.95 200 | 2 80 | 334 90
80 114 59.43 | 3340 1499 | 33.88 202 | 2 81 | 334 9 81 165 86.08 | 3340 1499 |
70.32 ` After the sequence alignment step completes, you will have an overlap
(`.ovl`) file that can be fed into the final stages of your assembler to
complete the consensus step.

## Tuning Suggestions

* As a rule of thumb, a single task should take a minute or two. If tasks are much longer than that, it becomes more difficult to measure progress and recover from failures. If tasks are much shorter than that, the overhead of managing the tasks becomes excessive. Use the `-n` parameter to increase or decrease the size of tasks. 
* When using banded alignment (the default), the `-q` match quality parameter has a significant effect on speed. A higher quality threshhold will consider more alignments, but take longer and produce more output. 
* The columns of the output are as follows: 
* Total Time is the elapsed time the master has been running. 
* Workers Idle is the number of workers that are connected, but do not have a task to run. 
* Workers Busy is the number of workers that are currently running a task. 
* Tasks Submitted is the cumulative number of tasks created by the master. 
* Tasks Idle is the number of tasks waiting for a worker. 
* Tasks Running is the number of tasks currently running on a worker. 
* Tasks Done is the cumulative number of tasks completed. 
* Avg Time is the average time a task takes to run. An average time of 60 seconds is a good goal. 
* K-Cand Loaded indicates the number of candidates loaded into memory (in thousands). 
* K-Seqs Loaded indicates the number of sequences loaded into memory (in thousands). 
* Speedup is the approximate speed of the distributed framework, relative to one processor. 

## For More Information

For the latest information about SAND, please visit our [web
site](http://ccl.cse.nd.edu/software/sand) and subscribe to our [mailing
list](http://ccl.cse.nd.edu/software).

