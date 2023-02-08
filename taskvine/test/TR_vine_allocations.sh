#!/bin/sh
set -ex

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PATH=$(pwd)/../src/worker:$(pwd)/../../batch_job/src:$PATH
export PYTHONPATH=$(pwd)/../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=vine.status
PORT_FILE=vine.port

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	return 0
}

run()
{
	# worker resources (used by worker in factory in wq_alloc_test.py):
	cores=4
	memory=2000
	disk=2000
	gpus=8

	# send makeflow to the background, saving its exit status.
	${CCTOOLS_PYTHON_TEST_EXEC} vine_alloc_test.py $PORT_FILE $cores $memory $disk $gpus; echo $? > $STATUS_FILE

	# retrieve wq script exit status
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

# vim: set noexpandtab tabstop=4:
