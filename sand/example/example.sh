#!/bin/bash

CCTOOLS_HOME=../..
SAND_HOME=../

# link to all the necessary pieces.
echo "Getting compress_reads"
ln -s ${SAND_HOME}/formatting/src/compress_reads ./compress_reads || { echo "Please build sand first."; exit 1 ; }

echo "Getting worker"
ln -s ${CCTOOLS_HOME}/dttools/src/worker ./worker || { echo "Please build dttools first."; exit 1 ; }

echo "Getting serial filter program"
ln -s ${SAND_HOME}/filtering/src/filter_mer_seq ./filter_mer_seq || { echo "Please build sand first."; exit 1 ; }

echo "Getting filter master"
ln -s ${SAND_HOME}/filtering/src/sand_filter_master ./sand_filter_master || { echo "Please build sand first."; exit 1 ; }

echo "Getting serial alignment program"
ln -s ${SAND_HOME}/alignment/src/sw_alignment ./sw_alignment || { echo "Please build sand first."; exit 1 ; }

echo "Getting alignment master"
ln -s ${SAND_HOME}/alignment/src/sand_align_master ./sand_align_master || { echo "Please build sand first."; exit 1 ; }

# compress reads
echo "Compressing reads"
./compress_reads < test_20.fa > test_20.cfa

# do candidate selection
echo "Starting worker for filtering"
./worker -t 5s -d all localhost 9090 &
wpid=$!
echo "Worker is process $wpid"

echo "Starting filter master"
./sand_filter_master -s 10 -p 9090 -b test_20.cfa test_20.cand || { echo "Error in filtering."; kill -9 $wpid; exit 1 ; }

echo "Waiting for worker to exit"
wait $wpid

# do alignment
echo "Starting worker for alignment"
./worker -t 5s -d all localhost 9090 &
wpid=$!
echo "Worker is process $wpid"

echo "Starting assembly_master"
./sand_align_master -n 1 -p 9090 sw_alignment test_20.cand test_20.cfa test_20.ovl || { echo "Error in alignment."; kill -9 $wpid; exit 1 ; }
echo "Checking results"
diff --brief test_20.ovl test_20.right && echo "Files test_20.ovl and test_20.right are the same";
echo "Waiting for worker to exit"
wait $wpid
echo "Removing created files"
rm -f compress_reads worker filter_mer_seq filter_master sw_alignment sand_align test_20.cand test_20.cfa;
rm -i test_20.ovl;

exit 0;
