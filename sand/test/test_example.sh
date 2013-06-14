#!/bin/sh

source ../../dttools/src/test_runner.common.sh

PATH=../src:../../dttools/src:$PATH
export PATH

echo "SAND short assembly test"

echo "Cleaning up old data"
rm -rf test_20.cand.filter.tmp test_20.cfa test_20.cand test_20.sw.ovl test_20.banded.ovl port.file

echo "Compressing reads"
sand_compress_reads < test_20.fa > test_20.cfa

echo "Starting filter master..."
rm -f port.file
sand_filter_master -s 10 -Z port.file -d all -o filter.log test_20.cfa test_20.cand &

run_local_worker port.file
require_identical_files test_20.cand test_20.cand.right

echo "Starting Smith-Waterman assembly..."
rm -f port.file
sand_align_master -d all -o sw.log -Z port.file -e "-a sw" sand_align_kernel test_20.cand test_20.cfa test_20.sw.ovl &

run_local_worker port.file
require_identical_files test_20.sw.ovl test_20.sw.right

echo "Starting banded assembly..."
rm -f port.file
sand_align_master -d all -o banded.log -Z port.file -e "-a banded" sand_align_kernel test_20.cand test_20.cfa test_20.banded.ovl &

run_local_worker port.file
require_identical_files test_20.banded.ovl test_20.banded.right

echo "Test assembly complete."

exit 0

