include(manual.h)dnl
HEADER(makeflow_blast)

SECTION(NAME) 
BOLD(makeflow_blast) - Generate a Makeflow to parallelize and distribute blastall jobs 

SECTION(SYNOPSIS)
CODE(BOLD(makeflow_blast query_granularity character_granularity [blast_options]))

SECTION(DESCRIPTION)
BOLD(makeflow_blast) is a script to generate MANPAGE(makeflow) workflows to execute blastall jobs. Essentially, the script uses query_granularity (the maximum number of sequences per fasta file split) and character_granularity (the maximum number of characters per fasta file split) to determine how to break up the input fasta file.  It then creates a makeflow that will execute a blastall with the desired parameters on each part and concatenate the results into the desired output file.  For simplicity, all of the arguments following query_granularity and character_granularity are passed through as the options to MANPAGE(blastall).
PARA
BOLD(makeflow_blast) executes a small test BLAST job with the user provided parameters in order to be sure that the given parameters are sane.  It then calculates the number of parts the provided fasta input file will require, prints a makeflow rule to generate those parts using MANPAGE(split_fasta), and enumerates makeflow rules to execute blastall with the given parameters on each part. Subsequent rules to condense and clean the intermediate input and output are then produced.
PARA
BOLD(makeflow_blast) expects a blastall in the path, and should be used from the directory containing the input files and databases.  For distribution convenience, it is required that the files constituting a given BLAST database must be stored in a folder with the same name as that database.

SECTION(OPTIONS)
OPTIONS_BEGIN
OPTION_PAIR(-i, input)Specifiy the input fasta file for querying the BLAST database
OPTION_PAIR(-o, output)Specify the output file for final results
OPTION_PAIR(-d, databse)Specify the BLAST database to be queried
OPTIONS_END

SECTION(EXIT STATUS)
On success, returns zero.  On failure, returns non-zero.

SECTION(ENVIRONMENT VARIABLES)

SECTION(EXAMPLES)

To generate a makeflow to run blastall -p blastn on smallpks.fa and testdb, splitting smallpks.fa every 500 sequences or 10000 characters and placing the blast output into test.txt do:
LONGCODE_BEGIN
python makeflow_blast 500 10000 -i smallpks.fa -o test -d testdb/testdb -p blastn > Makeflow
LONGCODE_END
You can then execute this workflow in a variety of distributed and parallel environments using the makeflow command.

SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

SECTION(SEE ALSO)

SEE_ALSO_MAKEFLOW

FOOTER

