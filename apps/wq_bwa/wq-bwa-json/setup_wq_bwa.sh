#!/usr/bin/bash

#Copyright (C) 2020- The University of Notre Dame
#This software is distributed under the GNU General Public License.
#See the file COPYING for details.

./fastq_generate.pl 100000 1000 > ref.fastq
echo "done with ref.fastq"

#./fastq_generate.pl 10000000 10 > query.fastq      #5 splits
./fastq_generate.pl 100000000 10 > query.fastq      #50 splits
echo "done with query.fastq"

./bwa index ref.fastq
echo "done with indexing"

./create_splits.py
echo "done with splits"

gzip query.fastq.*
echo "done with compression"
