#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

export PATH=../src/tools:../src/worker:$PATH

STATUS_FILE=vine_test.status
PORT_FILE=vine_test.port
LOG_FILE=vine_test.log

# This test is C-only (it drives ../src/tools/vine_test_stdout_capture
# through the taskvine C API) so, unlike most of the other TR_vine_* tests,
# it does not need the Python bindings and always runs.

prepare()
{
	rm -f $STATUS_FILE $PORT_FILE $LOG_FILE

	return 0
}

run()
{
	# send the manager to the background, saving its exit status.
	(../src/tools/vine_test_stdout_capture -Z $PORT_FILE > $LOG_FILE 2>&1; echo $? > $STATUS_FILE) &

	echo "waiting for manager to get ready"
	wait_for_file_creation $PORT_FILE 5

	run_taskvine_worker $PORT_FILE worker.log

	echo "waiting for manager to finish"
	wait_for_file_creation $STATUS_FILE 15

	echo "=== vine_test_stdout_capture output ==="
	cat $LOG_FILE

	status=$(cat $STATUS_FILE)
	if [ "$status" -ne 0 ]
	then
		echo "worker log:"
		cat worker.log
		exit 1
	fi

	exit 0
}

clean()
{
	rm -f $STATUS_FILE $PORT_FILE $LOG_FILE worker.log
	rm -rf vine-run-info

	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
