#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(dirname "${CCTOOLS_PYTHON_TEST_EXEC}"):$PATH

STATUS_FILE=vine_graph_workflow_examples.status
PORT_FILE=vine_graph_workflow_examples.port
RESULT_FILE=vine_graph_workflow_examples.result

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle" || return 1
	return 0
}

prepare()
{
	rm -f $STATUS_FILE $PORT_FILE $RESULT_FILE worker.log
	return 0
}

run()
{
	( ${CCTOOLS_PYTHON_TEST_EXEC} vine_graph_workflow_examples.py $PORT_FILE --case chain-branches:6 --task-group 1 --result-file $RESULT_FILE --no-print-results --timeout 90; echo $? > $STATUS_FILE ) &

	wait_for_file_creation $PORT_FILE 15

	cores=16
	memory=2000
	disk=2000
	run_taskvine_worker $PORT_FILE worker.log

	wait_for_file_creation $STATUS_FILE 60

	status=$(cat $STATUS_FILE)
	if [ $status -ne 0 ]
	then
		exit 1
	fi

	test -s $RESULT_FILE
	exit 0
}

clean()
{
	rm -f $STATUS_FILE $PORT_FILE $RESULT_FILE worker.log
	rm -rf vine-run-info
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
