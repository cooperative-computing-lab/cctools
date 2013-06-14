include(manual.h)dnl
HEADER(sand_filter_master)

SECTION(NAME)
BOLD(sand_filter_master) - filter sequences for alignment in parallel

SECTION(SYNOPSIS)
CODE(BOLD(sand_filter_master [options] sequences.cfa candidates.cand))

SECTION(DESCRIPTION)

BOLD(sand_filter_master) is the first step in the SAND assembler.
It reads in a body of sequences, and uses a linear-time algorithm
to produce a list of candidate sequences to be aligned in detail
by MANPAGE(sand_align_master,1).
PARA
This program uses the Work Queue system to distributed tasks
among processors.  After starting BOLD(sand_filter_master),
you must start a number of MANPAGE(work_queue_worker,1) processes
on remote machines.  The workers will then connect back to the
master process and begin executing tasks.  The actual filtering
is performed by MANPAGE(sand_filter_kernel,1) on each machine.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_PAIR(-p,port)Port number for queue master to listen on. (default: 9123)
OPTION_PAIR(-s,size)Number of sequences in each filtering task. (default: 1000)
OPTION_PAIR(-r,file)A meryl file of repeat mers to be filtered out.
OPTION_PAIR(-R,n)Automatically retry failed jobs up to n times. (default: 100)
OPTION_PAIR(-k,size)The k-mer size to use in candidate selection (default is 22).
OPTION_PAIR(-w,size)The minimizer window size. (default is 22).
OPTION_ITEM(-u)If set, do not unlink temporary binary output files.
OPTION_PAIR(-c,file)Checkpoint filename; will be created if necessary.
OPTION_PAIR(-d,flag)Enable debugging for this subsystem.  (Try BOLD(-d all) to start.)
OPTION_PAIR(-F,number)Work Queue fast abort multiplier.     (default is 10.)
OPTION_PAIR(-Z,file)Select port at random and write it out to this file.
OPTION_PAIR(-o,file)Send debugging to this file.
OPTION_ITEM(-v)Show version string
OPTION_ITEM(-h)Show this help screen
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

If you begin with a FASTA formatted file of reads,
used MANPAGE(sand_compress_reads,1) to produce a
compressed FASTA (cfa) file.  To run filtering sequentially,
start a single MANPAGE(work_queue_worker,1) process in the background.
Then, invoke BOLD(sand_filter_master).

LONGCODE_BEGIN
% sand_compress_reads mydata.fasta mydata.cfa
% work_queue_worker localhost 9123 &
% sand_filter_master mydata.cfa mydata.cand
LONGCODE_END

To speed up the process, run more MANPAGE(work_queue_worker,1) processes
on other machines, or use MANPAGE(condor_submit_workers,1) or MANPAGE(sge_submit_workers,1) to start hundreds of workers in your local batch system.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_SAND

FOOTER

