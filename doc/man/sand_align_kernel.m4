include(manual.h)dnl
HEADER(sand_align_kernel)

SECTION(NAME)
BOLD(sand_align_kernel) - align candidate sequences sequentially

SECTION(SYNOPSIS)
CODE(BOLD(sand_align_kernel [options] [input file]))

SECTION(DESCRIPTION)

BOLD(sand_align_kernel) aligns candidate sequences sequentially.
It is not normally called by the user, but is invoked by
MANPAGE(sand_align_master,1) for each sequential step of a distributed
alignment workload.  The options to BOLD(sand_align_kernel) control
the type of alignment (Smith-Waterman, Prefix-Suffix, Banded, etc)
and the quality threshhold for reporting alignments.  These options
are typically passed in by giving the BOLD(-e) option to BOLD(sand_align_master).
PARA
BOLD(sand_align_kernel) reads a list of sequences
from the given input file, or from standard input if none is given.
The sequences are in the compressed fasta format produced
by MANPAGE(sand_compress_reads,1).  The first sequence in the input
is compared against all following sequences until a separator line.
Following the separator, the next sequence is compared against
all following sequences until the following separator, and so on.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_PAIR(-a,sw|ps|banded)Specify the type of alignment: sw (Smith-Waterman), ps (Prefix-Suffix), or banded.  If not specified, default is banded.
OPTION_PAIR(-o,ovl|ovl_new|align|matrix)Specify how each alignment should be output: ovl (Celera V5, V6 OVL format), ovl_new (Celera V7 overlap format), align (display the sequences and alignment graphically) or matrix (display the dynamic programming matrix).  MANPAGE(sand_align_master,1) expects the ovl output format, which is the default.  The other formats are useful for debugging.
OPTION_PAIR(-m,length)Minimum aligment length (default: 0).
OPTION_PAIR(-q,quality)Minimum match quality (default: 1.00)
OPTION_ITEM(-x)Delete input file after completion.
OPTION_PAIR(-d,subsystem)Enable debugging for this subsystem.  (Try BOLD(-d all) to start.
OPTION_ITEM(-v)Show program version.
OPTION_ITEM(-h)Display this message.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

Users do not normally invoke sand_align_kernel directly.  Instead, pass arguments by using the BOLD(-e) option to MANPAGE(sand_align_master,1).  For example, to specify a minimum alignment length of 5 and a minimum quality of 0.25:

LONGCODE_BEGIN
% sand_align_master sand_align_kernel -e "-m 5 -q 0.25" mydata.cand mydata.cfa mydata.ovl
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_SAND

FOOTER

