#!/bin/bash

port=9092

PATH=../src:../../dttools/src:$PATH
export PATH

error_state=0;

echo "Cleaning up old data"
rm -f test_20.cfa test_20.cand test_20.sw.ovl test_20.banded.ovl

echo "Compressing reads"
sand_compress_reads < test_20.fa > test_20.cfa

echo "Starting worker for filtering"
work_queue_worker -t 60s -d all -o worker.log localhost $port &
wpid=$!
echo "Worker is process $wpid"

echo "Starting filter master"
sand_filter_master -s 10 -p $port -d all -o filter.log test_20.cfa test_20.cand || { echo "Error in filtering."; kill -9 $wpid; exit 1 ; }

echo "Checking filtering results..."
diff --brief test_20.cand test_20.cand.right && echo "Candidates match" || { kill -9 $wpid ; exit 1; }

echo "Starting Smith-Waterman assembly..."
sand_align_master -d all -o sw.log -p $port -e "-a sw" sand_align_kernel test_20.cand test_20.cfa test_20.sw.ovl || { echo "Error in alignment."; kill -9 $wpid; exit 1 ; }

echo "Checking Smith-Waterman results"
diff --brief test_20.sw.ovl test_20.sw.right && echo "Files test_20.sw.ovl and test_20.sw.right are the same" || { kill -9 $wpid ; exit 1 ; }

echo "Starting banded assembly..."
sand_align_master -d all -o banded.log -p $port -e "-a banded" sand_align_kernel test_20.cand test_20.cfa test_20.banded.ovl || { echo "Error in alignment."; kill -9 $wpid; exit 1 ; }

echo "Checking banded results"
diff --brief test_20.banded.ovl test_20.banded.right && echo "Files test_20.banded.ovl and test_20.banded.right are the same" || { kill -9 $wpid ; exit 1 ; }

echo "Stopping the worker..."
kill -9 $wpid
wait $wpid

exit 0;
