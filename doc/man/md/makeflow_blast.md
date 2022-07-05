






















# makeflow_blast(1)

## NAME
**makeflow_blast** - Generate a Makeflow to parallelize and distribute blastall jobs

## SYNOPSIS
**makeflow_blast query_granularity character_granularity [blast_options]**

## DESCRIPTION
**makeflow_blast** is a script to generate [makeflow()](makeflow.md) workflows to execute **blastall** jobs. Essentially, the script uses query_granularity (the maximum number of sequences per fasta file split) and character_granularity (the maximum number of characters per fasta file split) to determine how to break up the input fasta file.  It then creates a makeflow that will execute a blastall with the desired parameters on each part and concatenate the results into the desired output file.  For simplicity, all of the arguments following query_granularity and character_granularity are passed through as the options to **blastall**

**makeflow_blast** executes a small test BLAST job with the user provided parameters in order to be sure that the given parameters are sane.  It then calculates the number of parts the provided fasta input file will require, prints a makeflow rule to generate those parts using [split_fasta()](split_fasta.md), and enumerates makeflow rules to execute blastall with the given parameters on each part. Subsequent rules to condense and clean the intermediate input and output are then produced.

**makeflow_blast** expects a blastall in the path, and should be used from the directory containing the input files and databases.  For distribution convenience, it is required that the files constituting a given BLAST database must be stored in a folder with the same name as that database.

## OPTIONS

- **-i** _&lt;input&gt;_<br />Specifiy the input fasta file for querying the BLAST database
- **-o** _&lt;output&gt;_<br />Specify the output file for final results
- **-d** _&lt;databse&gt;_<br />Specify the BLAST database to be queried


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## ENVIRONMENT VARIABLES

## EXAMPLES

To generate a makeflow to run blastall -p blastn on smallpks.fa and testdb, splitting smallpks.fa every 500 sequences or 10000 characters and placing the blast output into test.txt do:
```
python makeflow_blast 500 10000 -i smallpks.fa -o test -d testdb/testdb -p blastn > Makeflow
```
You can then execute this workflow in a variety of distributed and parallel environments using the makeflow command.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools
