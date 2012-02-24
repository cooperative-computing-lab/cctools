include(manual.h)dnl
HEADER(split_fasta)

SECTION(NAME) 
BOLD(split_fasta) - Split a fasta file according to sequence and character counts

SECTION(SYNOPSIS)
CODE(BOLD(split_fasta query_granularity character_granularity fasta_file))

SECTION(DESCRIPTION)
BOLD(split_fasta) is a simple script to split a fasta file according to user provided parameters.  The script iterates over the given file, generating a new sub_file called input.i each time the contents of the previous file (input.(i-1)) exceed the number of queries given by query_granularity or the number of characters given by character_granularity. 

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(ENVIRONMENT VARIABLES)

SECTION(EXAMPLES)

To split a fasta file smallpks.fa into pieces no larger than 500 queries and with no piece receiving additional sequences if it exceeds 10000 characters we would do:
LONGCODE_BEGIN
python split_fasta 500 10000 smallpks.fa
LONGCODE_END
This would generate files input.0, input.1, ..., input.N where N is the number of appropriately constrained files necessary to contain all sequences in smallpks.fa.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER

