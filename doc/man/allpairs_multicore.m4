include(manual.h)dnl
HEADER(allpairs_multicore)

SECTION(NAME)
BOLD(allpairs_multicore) - executes All-Pairs workflow in parallel on a multicore machine 

SECTION(SYNOPSIS)
CODE(BOLD(allparis_multicore [options] PARAM(set A) PARAM(set B) PARAM(compare function)))

SECTION(DESCRIPTION)

BOLD(allpairs_multicore) computes the Cartesian product of two sets
(BOLD(PARAM(set A)) and BOLD(PARAM(set B))), generating a matrix where each cell
M[i,j] contains the output of the function F (BOLD(PARAM(compare function))) on
objects A[i] (an item in BOLD(PARAM(set A))) and B[j] (an item in
BOLD(PARAM(set B))). The resulting matrix is displayed on the standard output,
one comparison result per line along with the associated X and Y indices. 
PARA
For large sets of objects, BOLD(allpairs_multicore) will use as many cores as
you have available, and will carefully manage virtual memory to exploit
locality and avoid thrashing. Because of this, you should be prepared for the
results to come back in any order. If you want to further exploit the
parallelism of executing All-Pairs workflows on multiple (multicore) machines,
please refer to the MANPAGE(allpairs_master,1) utility.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_PAIR(-b, items)Block size: number of items to hold in memory at once. (default: 50% of RAM)
OPTION_PAIR(-c, cores)Number of cores to be used. (default: # of cores in machine)
OPTION_PAIR(-e, args)Extra arguments to pass to the comparison program.
OPTION_PAIR(-d, flag)Enable debugging for this subsystem.
OPTION_ITEM(-v)Show program version.
OPTION_ITEM(-h)Display this message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Let's suppose you have a whole lot of files that you want to compare all to
each other, named CODE(a), CODE(b), CODE(c), and so on. Suppose that you also
have a program named BOLD(CODE(compareit)) that when invoked as CODE(compareit a b)
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

Then, invoke BOLD(allpairs_multicore) like this:

LONGCODE_BEGIN
 % allpairs_multicore set.list set.list compareit
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

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(All-Pairs User Manual,"http://www.nd.edu/~ccl/software/manuals/allpairs.html")
LIST_ITEM MANPAGE(allpairs_master,1)
LIST_END
