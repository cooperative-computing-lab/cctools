#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=ds.status
PORT_FILE=ds.port

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -rf input.file
	rm -rf output.file
	rm -rf executable.file

	/bin/echo hello world > input.file
	/bin/cp /bin/echo executable.file

	mkdir -p testdir
	cp input.file executable.file testdir

	return 0
}

run()
{
	# send makeflow to the background, saving its exit status.
	( ${CCTOOLS_PYTHON_TEST_EXEC} vine_test.py $PORT_FILE; echo $? > $STATUS_FILE ) &

	# wait at most 15 seconds for ds to find a port.
	wait_for_file_creation $PORT_FILE 15

	run_ds_worker $PORT_FILE worker.log

	# wait for ds to exit.
	wait_for_file_creation $STATUS_FILE 15

	# retrieve exit status
	status=$(cat $STATUS_FILE)
	if [ $status -ne 0 ]
	then
		exit 1
	fi

	exit 0
}

clean()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -rf input.file
	rm -rf output.file
	rm -rf executable.file
	rm -rf testdir

	exit 0
}


dispatch "$@"
