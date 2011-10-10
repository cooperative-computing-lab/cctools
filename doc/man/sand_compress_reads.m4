include(manual.h)dnl
HEADER(sand_compress_reads)

SECTION(NAME)
BOLD(sand_compress_reads) - compress sequence data

SECTION(SYNOPSIS)
CODE(BOLD(sand_compress_reads [-qcivh] [infile] [outfile]))

SECTION(DESCRIPTION)

BOLD(sand_compress_reads) reads sequence data in standard FASTA format,
and produces output in the compressed FASTA (cfa) format used by
the MANPAGE(sand_filter_master,1) and MANPAGE(sand_align_master,1).
PARA
If the output file is omitted, standard output is used.
if the input file is omitted, standard input is used.
After completing the compression, this program will output a summary
line of sequences and bytes compressed.

SECTION(OPTIONS)

OPTIONS_BEGIN
OPTION_ITEM(-q) Quiet mode: suppress summary line.
OPTION_ITEM(-c) Remove Celera read ids if data came from the Celera gatekeeper.
OPTION_ITEM(-i) Remove Celera read ids but leave internal ids if the data came from the Celera gatekeeper.
OPTION_ITEM(-h) Display version information.
OPTION_ITEM(-v) Show help text.
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(EXAMPLES)

To compress CODE(mydata.fasta) into CODE(mydata.cfa):

LONGCODE_BEGIN
% sand_compress_reads mydata.fasta mydata.cfa
LONGCODE_END

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_SAND
