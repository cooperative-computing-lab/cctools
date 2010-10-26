#!/bin/bash
# Note: cctools has to be installed to execute this script.
# Generate candidates from FASTA file "random.fa". The candidates will be saved
# in file "random.cand". The format of each line in the .cand file is: 
# read1_name   read2_name   direction   start_position_in_read1   start_position_in_read2

port=9091

PATH=../../src:../../../dttools/src:$PATH
export PATH

error_state=0;

rm -f random.cfa
rm -f random.cand 
rm -rf random.cand.output
rm -f filter.log
rm -f worker.log

echo "Compressing reads ..."
sand_compress_reads < random.fa > random.cfa

echo "Starting worker for filtering ..."
worker -t 5s -d all -o worker.log localhost $port &
wpid=$!
echo "Worker is process $wpid"

echo "Starting filter master ..."
sand_filter_master -s 100 -k 22 -p $port random.cfa random.cand -d all -o filter.log || { echo "Error in filtering."; kill -9 $wpid; exit 1 ; }

echo "Waiting for worker to exit"
wait $wpid

exit 0;
