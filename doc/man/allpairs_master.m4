include(manual.h)dnl
HEADER(allpairs_master)

SECTION(NAME)
BOLD(allpairs_master) - executes All-Pairs workflow in parallel on distributed systems

SECTION(SYNOPSIS)
CODE(BOLD(allparis_master [options] PARAM(set A) PARAM(set B) PARAM(compare function)))

SECTION(DESCRIPTION)

BOLD(allpairs_master) computes the Cartesian product of two sets
(BOLD(PARAM(set A)) and BOLD(PARAM(set B))), generating a matrix where each cell
M[i,j] contains the output of the function F (BOLD(PARAM(compare function))) on
objects A[i] (an item in BOLD(PARAM(set A))) and B[j] (an item in
BOLD(PARAM(set B))). The resulting matrix is displayed on the standard output,
one comparison result per line along with the associated X and Y indices. 
PARA
BOLD(allpairs_master) uses the Work Queue system to distribute tasks among
processors.  Each processor utilizes the MANPAGE(allpairs_multicore,1) program
to execute the tasks in parallel if multiple cores are present. After starting
BOLD(allpairs_master), you must start a number of MANPAGE(work_queue_worker,1)
processes on remote machines.  The workers will then connect back to the master
process and begin executing tasks.  

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_TRIPLET(-p,port,port)The port that the master will be listening on.
OPTION_TRIPLET(-e,extra-args,args)Extra arguments to pass to the comparison function.
OPTION_TRIPLET(-f,input-file,file)Extra input file needed by the comparison function. (may be given multiple times)
OPTION_TRIPLET(-o,debug-file,file)Write debugging output to this file (default to standard output)
OPTION_TRIPLET(-t,estimated-time,seconds)Estimated time to run one comparison. (default chosen at runtime)
OPTION_TRIPLET(-x,width,item)Width of one work unit, in items to compare. (default chosen at runtime)
OPTION_TRIPLET(-y,height,items)Height of one work unit, in items to compare. (default chosen at runtime)
OPTION_TRIPLET(-N,project-name,project)Report the master information to a catalog server with the project name - PARAM(project)
OPTION_TRIPLET(-P,priority,integer)Priority. Higher the value, higher the priority.
OPTION_TRIPLET(-d,debug,flag)Enable debugging for this subsystem. (Try -d all to start.)
OPTION_ITEM(`-v, --version')Show program version.
OPTION_PAIR(`-h, --help')Display this message.
OPTION_TRIPLET(-Z,port-file,file)Select port at random and write it to this file.  (default is disabled)
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Let's suppose you have a whole lot of files that you want to compare all to
each other, named CODE(a), CODE(b), CODE(c), and so on. Suppose that you also
have a program named BOLD(compareit) that when invoked as CODE(compareit a b)
will compare files CODE(a) and CODE(b) and produce some output summarizing the
difference between the two, like this: 

LONGCODE_BEGIN
 a b are 45 percent similar
LONGCODE_END

To use the allpairs framework, create a file called CODE(set.list) that lists each of
your files, one per line: 

LONGCODE_BEGIN
 a
 b
 c
 ...
LONGCODE_END

Because BOLD(allpairs_master) utilizes MANPAGE(allpairs_multicore,1), so please
make sure MANPAGE(allpairs_multicore,1) is in your PATH before you proceed.To run
a All-Pairs workflow sequentially, start a single MANPAGE(work_queue_worker,1)
process in the background. Then, invoke BOLD(allpairs_master). 

LONGCODE_BEGIN
 % work_queue_worker localhost 9123 &
 % allpairs_master set.list set.list compareit
LONGCODE_END

The framework will carry out all possible comparisons of the objects, and print
the results one by one (note that the first two columns are X and Y indices in
the resulting matrix): 

LONGCODE_BEGIN
 1	1	a a are 100 percent similar
 1	2	a b are 45 percent similar
 1	3	a c are 37 percent similar
 ...
LONGCODE_END

To speed up the process, run more MANPAGE(work_queue_worker,1) processes on
other machines, or use MANPAGE(condor_submit_workers,1) or
MANPAGE(sge_submit_workers,1) to start hundreds of workers in your local batch
system. 
PARA
The following is an example of adding more workers to execute a All-Pairs
workflow. Suppose your BOLD(allpairs_master) is running on a machine named
barney.nd.edu. If you have access to login to other machines, you could simply
start worker processes on each one, like this:

LONGCODE_BEGIN
 % work_queue_worker barney.nd.edu 9123
LONGCODE_END

If you have access to a batch system like Condor, you can submit multiple
workers at once:

LONGCODE_BEGIN
 % condor_submit_workers barney.nd.edu 9123 10
 Submitting job(s)..........
 Logging submit event(s)..........
 10 job(s) submitted to cluster 298.
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(All-Pairs User Manual,"http://www.nd.edu/~ccl/software/manuals/allpairs.html")
LIST_ITEM LINK(Work Queue User Manual,"http://www.nd.edu/~ccl/software/manuals/workqueue.html")
LIST_ITEM MANPAGE(work_queue_worker,1)
LIST_ITEM MANPAGE(condor_submit_workers,1)
LIST_ITEM MANPAGE(sge_submit_workers,1)
LIST_ITEM MANPAGE(allpairs_multicore,1)
LIST_END

FOOTER

