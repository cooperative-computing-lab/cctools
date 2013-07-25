include(manual.h)dnl
HEADER(wavefront_master)

SECTION(NAME)
BOLD(wavefront_master) - executes Wavefront workflow in parallel on distributed systems

SECTION(SYNOPSIS)
CODE(BOLD(wavefront [options] PARAM(command) PARAM(xsize) PARAM(ysize) PARAM(inputdata) PARAM(outputdata)))

SECTION(DESCRIPTION)

BOLD(wavefront_master) computes a two dimensional recurrence relation. You
provide a function F (BOLD(PARAM(command))) that accepts the left (x), right
(y), and diagonal (d) values and initial values (BOLD(PARAM(inputdata))) for
the edges of the matrix. The output matrix, whose size is determined by
BOLD(PARAM(xsize)) and BOLD(PARAM(ysize)), will be stored in a file specified
by BOLD(PARAM(outputdata)).
PARA
BOLD(wavefront_master) uses the Work Queue system to distribute tasks among
processors. After starting BOLD(wavefront_master), you must start a number of
MANPAGE(work_queue_worker,1) processes on remote machines.  The workers will
then connect back to the master process and begin executing tasks.  

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(`-h, --help')Show this help screen
OPTION_ITEM(`-v, --version')Show version string
OPTION_TRIPLET(-d, debug, subsystem)Enable debugging for this subsystem. (Try -d all to start.)
OPTION_TRIPLET(-N, project-name, project)Set the project name to <project>
OPTION_TRIPLET(-o, debug-file, file)Send debugging to this file.
OPTION_TRIPLET(-p, port, port)Port number for queue master to listen on.
OPTION_TRIPLET(-P, priority, num)Priority. Higher the value, higher the priority.
OPTION_TRIPLET(-Z, port-file, file)Select port at random and write it to this file.  (default is disabled)
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Suppose you have a program named CODE(function) that you want to use in the
Wavefont workflow computation. The program CODE(function), when invoked as
CODE(function a b c), should do some computations on files CODE(a), CODE(b) and
CODE(c) and produce some output on the standard output. 
PARA
Before running BOLD(wavefront_master), you need to create a file, say
CODE(input.data), that lists initial values of the matrix (values on the left
and bottom edges), one per line: 

LONGCODE_BEGIN
 0	0	value.0.0
 0	1	value.0.1
 ...
 0	n	value.0.n
 1	0	value.1.0
 2	0	value.2.0
 ...
 n	0	value.n.0
LONGCODE_END

To run a Wavefront workflow sequentially, start a single
MANPAGE(work_queue_worker,1) process in the background. Then, invoke
BOLD(wavefront_master). The following example computes a 10 by 10 Wavefront
matrix:

LONGCODE_BEGIN
 % work_queue_worker localhost 9123 &
 % wavefront_master function 10 10 input.data output.data
LONGCODE_END

The framework will carry out the computations in the order of dependencies, and
print the results one by one (note that the first two columns are X and Y
indices in the resulting matrix) in the specified output file. Below is an
example of what the output file - CODE(output.data) would look like: 

LONGCODE_BEGIN
 1	1	value.1.1	
 1	2	value.1.2	
 1	3	value.1.3	
 ...
LONGCODE_END

To speed up the process, run more MANPAGE(work_queue_worker,1) processes on
other machines, or use MANPAGE(condor_submit_workers,1) or
MANPAGE(sge_submit_workers,1) to start hundreds of workers in your local batch
system. 
PARA
The following is an example of adding more workers to execute a Wavefront 
workflow. Suppose your BOLD(wavefront_master) is running on a machine named
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
LIST_ITEM LINK(Wavefront User Manual,"http://www.nd.edu/~ccl/software/manuals/wavefront.html")
LIST_ITEM LINK(Work Queue User Manual,"http://www.nd.edu/~ccl/software/manuals/workqueue.html")
LIST_ITEM MANPAGE(work_queue_worker,1)
LIST_ITEM MANPAGE(condor_submit_workers,1)
LIST_ITEM MANPAGE(sge_submit_workers,1)
LIST_END

FOOTER

