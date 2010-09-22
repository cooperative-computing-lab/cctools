#!/bin/bash

CCTOOLS_HOME=../..
SAND_HOME=../

error_state=0;

# link to all the necessary pieces.
echo "Getting compress_reads"
ln -s ${SAND_HOME}/src/sand_compress_reads ./sand_compress_reads || { echo "Please build sand first or delete conflicting file ./compress_reads ."; error_state=1 ; }
if [ ! -f ./sand_compress_reads ]; then echo "Please build sand first."; error_state=1 ; fi

echo "Getting worker"
ln -s ${CCTOOLS_HOME}/dttools/src/worker ./worker || { echo "Please build dttools first or delete conflicting file ./worker ."; error_state=1 ; }
if [ ! -f ./worker ]; then echo "Please build dttools first."; error_state=1 ; fi

echo "Getting serial alignment program"
ln -s ${SAND_HOME}/src/sand_sw_alignment ./sand_sw_alignment || { echo "Please build sand first or delete conflicting file ./sw_alignment ."; error_state=1 ; }
if [ ! -f ./sand_sw_alignment ]; then echo "Please build sand first."; error_state=1 ; fi

echo "Getting alignment master"
ln -s ${SAND_HOME}/src/sand_align_master ./sand_align_master || { echo "Please build sand first or delete conflicting file ./sand_align_master ."; error_state=1 ; }
if [ ! -f ./sand_align_master ]; then echo "Please build sand first."; error_state=1 ; fi

if(($error_state)); then
    rm -f sand_compress_reads worker sand_sw_alignment sand_align_master test_20.cand test_20.cfa;
    exit 1;
fi

# compress reads
echo "Compressing reads"
./sand_compress_reads < test_20.fa > test_20.cfa

# do alignment
echo "Starting worker for alignment"
./worker -t 5s -d all localhost 9090 &
wpid=$!
echo "Worker is process $wpid"

echo "Starting assembly_master"
./sand_align_master -n 1 -p 9090 sand_sw_alignment blank_task.cand test_20.cfa blank_task.ovl -d all || { echo "Error in alignment."; kill -9 $wpid; exit 1 ; }
echo "Checking results"
if [ -e blank_task.ovl -a ! -s blank_task.ovl ]; then
    echo "Correct empty result."
else
    echo "Error in alignment result."; kill -9 $wpid; exit 1;
fi

echo "Waiting for worker to exit"
wait $wpid
echo "Removing created files"
rm -f sand_compress_reads worker sand_sw_alignment sand_align_master test_20.cfa;
rm -f blank_task.ovl;

exit 0;
