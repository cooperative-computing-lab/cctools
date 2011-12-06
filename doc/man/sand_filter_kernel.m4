include(manual.h)dnl
HEADER(sand_filter_kernel)

SECTION(NAME)
BOLD(sand_filter_kernel) - filter read sequences sequentially

SECTION(SYNOPSIS)
CODE(BOLD(sand_filter_kernel [options] PARAM(sequence file) [second sequence file]))

SECTION(DESCRIPTION)

BOLD(sand_filter_kernel) filters a list of genomic sequences,
and produces a list of candidate pairs for more detailed alignment.
It is not normally called by the user, but is invoked by
MANPAGE(sand_filter_master,1) for each sequential step of a distributed
alignment workload.
PARA
If one sequence file is given, BOLD(sand_filter_kernel) will look for
similarities between all sequences in that file.  If given two files,
it will look for similarities between sequences in the first file and
the second file.  The output is a list of candidate pairs, listing the
name of the candidate sequences and a starting position for alignment.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_PAIR(-s,size)Size of "rectangle" for filtering. You can determine
the size dynamically by passing in d rather than a number.
OPTION_PAIR(-r,file)A meryl file of repeat mers to be ignored.
OPTION_PAIR(-k,size)The k-mer size to use in candidate selection (default is 22).
OPTION_PAIR(-w,number)The minimizer window size to use in candidate selection (default is 22).
OPTION_PAIR(-o,filename)The output file. Default is stdout.
OPTION_PAIR(-d,subsystem)Enable debug messages for this subsystem.  Try BOLD(-d all) to start.
OPTION_ITEM(-v)Show version string.
OPTION_ITEM(-h)Show help screen.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Users do not normally invoke BOLD(sand_filter_kernel) directly.  Instead, options such as the k-mer size, minimizer window, and repeat file may be specified by the same arguments to MANPAGE(sand_filter_master,1) instead.  For example, to run a filter with a k-mer size of 20, window size of 24, and repeat file of CODE(mydata.repeats):

LONGCODE_BEGIN
% sand_filter_master -k 20 -w 24 -r mydata.repeats mydata.cfa mydata.cand
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_SAND

FOOTER

