include(manual.h)dnl
HEADER(sand_uncompress_reads)

SECTION(NAME)
BOLD(sand_uncompress_reads) - uncompress sequence data

SECTION(SYNOPSIS)
CODE(BOLD(sand_uncompress_reads [-qvh] [infile] [outfile]))

SECTION(DESCRIPTION)

BOLD(sand_uncompress_reads) reads sequence data in compressed FASTA format (cfa) used by MANPAGE(sand_filter_master,1) and MANPAGE(sand_align_master,1).
and produces output in the standard FASTA format.
PARA
If the output file is omitted, standard output is used.
if the input file is omitted, standard input is used.
After completing the compression, this program will output a summary
line of sequences and bytes compressed.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-q) Quiet mode: suppress summary line.
OPTION_ITEM(-h) Display version information.
OPTION_ITEM(-v) Show help text.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To uncompress CODE(mydata.cfa) into CODE(mydata.fasta):

LONGCODE_BEGIN
% sand_uncompress_reads mydata.cfa mydata.fa
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

LIST_BEGIN
LIST_ITEM LINK(The Cooperative Computing Tools,"http://www.nd.edu/~ccl/software/manuals")
LIST_ITEM LINK(Parrot User Manual,"http://www.nd.edu/~ccl/software/manuals/parrot.html")
LIST_ITEM MANPAGE(sand_filter_master,1)
LIST_ITEM MANPAGE(sand_filter_kernel,1)
LIST_ITEM MANPAGE(sand_align_master,1)
LIST_ITEM MANPAGE(sand_align_kernel,1)
LIST_ITEM MANPAGE(sand_compress_reads,1)
LIST_ITEM MANPAGE(sand_uncompress_reads,1)
LIST_END
