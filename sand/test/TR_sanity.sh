#!/bin/sh

. ../../dttools/test/test_runner_common.sh

comp=test.cfa
cand=test.cand
sw_ovl=test.sw.ovl
banded_ovl=test.banded.ovl

worker_log=worker.log
sand_pid=sand.pid

sand_filter_debug=sand.filter.debug
sand_sw_debug=sand.sw.debug
sand_banded_debug=sand.banded.debug
sand_port=sand.port
sand_done=sand.done
sand_pid=sand.pid

prepare()
{
	clean
	return 0
}

run()
{
	(
		set -e

		PATH="../src:../../dttools/src:$PATH"
		export PATH

		echo "SAND short assembly test"

		echo "Compressing reads"
		sand_compress_reads < sand_sanity/test.fa > "$comp"

		echo "Starting filter master..."
		rm -f "$sand_port" "$sand_done" "$sand_pid"
		(sand_filter_master -s 10 -Z "$sand_port" -d all -o "$sand_filter_debug" "$comp" "$cand"; touch "$sand_done") &

		echo $! > $sand_pid
		run_local_worker "$sand_port" "$worker_log"
		wait_for_file_creation $sand_done 30
		kill $(cat $sand_pid) && exit 1

		require_identical_files "$cand" sand_sanity/test.cand.right

		echo "Starting Smith-Waterman assembly..."
		rm -f "$sand_port" "$sand_done" "$sand_pid"
		(sand_align_master -d all -o "$sand_sw_debug" -Z "$sand_port" -e "-a sw" sand_align_kernel "$cand" "$comp" "$sw_ovl"; touch "$sand_done") &

		echo $! > $sand_pid
		run_local_worker "$sand_port" "$worker_log"
		wait_for_file_creation $sand_done 30
		kill $(cat $sand_pid) && exit 1  # if we could kill it, then it did not finish the process correctly...

		require_identical_files "$sw_ovl" sand_sanity/test.sw.right

		echo "Starting banded assembly..."
		rm -f "$sand_port" "$sand_done" "$sand_pid"
		(sand_align_master -d all -o "$sand_banded_debug" -Z "$sand_port" -e "-a banded" sand_align_kernel "$cand" "$comp" "$banded_ovl"; touch "$sand_done") &

		echo $! > $sand_pid
		run_local_worker "$sand_port" "$worker_log"
		wait_for_file_creation $sand_done 30
		kill $(cat $sand_pid) && exit 1

		require_identical_files "$banded_ovl" sand_sanity/test.banded.right

		echo "Test assembly complete."
	)
	return $?
}

clean()
{
	rm -f "$comp" "$cand" "$sw_ovl" "$banded_ovl" "$sand_filter_debug" "$sand_sw_debug" "$sand_banded_debug" "$sand_port" "$worker_log" "$done_file"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
