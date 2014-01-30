#!/bin/sh

set -e

files="port.file random.cfa random.cand random.cand.output filter.log worker.log master.pid"

. ../../dttools/src/test_runner.common.sh

prepare()
{
	return 0
}

run()
{
	export PATH="$PATH:../src/"
	filter_verification/gen_random_sequence.pl 1000
	filter_verification/gen_random_reads.pl 1000
	sand_compress_reads < random.fa > random.cfa
	sand_filter_master -s 100 -k 22 -Z port.file -d all -o filter.log random.cfa random.cand &
	echo $! > master.pid
	run_local_worker port.file
	filter_verification/verify_candidates.pl
}

clean()
{
	kill -9 $(cat master.pid) || true
	rm -f $files
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
