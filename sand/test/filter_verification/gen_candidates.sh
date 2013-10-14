#!/bin/bash
# Note: cctools has to be installed to execute this script.
# Generate candidates from FASTA file "random.fa". The candidates will be saved
# in file "random.cand". The format of each line in the .cand file is: 
# read1_name   read2_name   direction   start_position_in_read1   start_position_in_read2

. ../../../dttools/src/test_runner.common.sh

PATH=../../src:../../../dttools/src:$PATH
export PATH

echo "Cleaning up..."
rm -rf port.file random.cfa random.cand random.cand.output filter.log worker.log

echo "Compressing reads ..."
sand_compress_reads < random.fa > random.cfa

echo "Starting filter master ..."
sand_filter_master -s 100 -k 22 -Z port.file -d all -o filter.log random.cfa random.cand &
echo $! > $1 # write pid of sand_filter_master so the process can be reaped in cleanup

run_local_worker port.file

exit 0;
