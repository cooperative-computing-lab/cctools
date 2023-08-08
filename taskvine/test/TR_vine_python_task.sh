#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(dirname "${CCTOOLS_PYTHON_TEST_EXEC}"):$PATH

STATUS_FILE=vine.status
PORT_FILE=vine.port

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle"  || return 1

	return 0
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	return 0
}

run()
{
	# send taskvine to the background, saving its exit status.
	( ${CCTOOLS_PYTHON_TEST_EXEC} vine_python_task.py $PORT_FILE; echo $? > $STATUS_FILE) &

	# wait at most 5 seconds for vine to find a port.
	wait_for_file_creation $PORT_FILE 5

	run_taskvine_worker $PORT_FILE worker.log

	# wait for vine to exit.
	wait_for_file_creation $STATUS_FILE 5

	# retrieve taskvine exit status
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

	rm -rf vine-run-info

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
