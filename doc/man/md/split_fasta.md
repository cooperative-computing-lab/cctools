






















# split_fasta(1)

## NAME
**split_fasta** - Split a fasta file according to sequence and character counts

## SYNOPSIS
**split_fasta query_granularity character_granularity fasta_file**

## DESCRIPTION
**split_fasta** is a simple script to split a fasta file according to user provided parameters.  The script iterates over the given file, generating a new sub_file called input.i each time the contents of the previous file (input.(i-1)) exceed the number of queries given by query_granularity or the number of characters given by character_granularity.

## OPTIONS



## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## ENVIRONMENT VARIABLES

## EXAMPLES

To split a fasta file smallpks.fa into pieces no larger than 500 queries and with no piece receiving additional sequences if it exceeds 10000 characters we would do:
```
python split_fasta 500 10000 smallpks.fa
```
This would generate files input.0, input.1, ..., input.N where N is the number of appropriately constrained files necessary to contain all sequences in smallpks.fa.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2021 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO


- [Cooperative Computing Tools Documentation]("../index.html")
- [Makeflow User Manual]("../makeflow.html")
- [makeflow(1)](makeflow.md) [makeflow_monitor(1)](makeflow_monitor.md) [makeflow_analyze(1)](makeflow_analyze.md) [makeflow_viz(1)](makeflow_viz.md) [makeflow_graph_log(1)](makeflow_graph_log.md) [starch(1)](starch.md) [makeflow_ec2_setup(1)](makeflow_ec2_setup.md) [makeflow_ec2_cleanup(1)](makeflow_ec2_cleanup.md)


CCTools 7.3.2 FINAL
