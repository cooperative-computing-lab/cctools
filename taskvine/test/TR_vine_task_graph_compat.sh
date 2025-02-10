#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH
export PATH=$(pwd)/../src/worker:$(pwd)/../../batch_job/src:$PATH

STATUS_FILE=vine.status
PORT_FILE=vine.port

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle; import dask"  || return 1

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
	${CCTOOLS_PYTHON_TEST_EXEC} vine_task_graph_compat.py
	echo $? > $STATUS_FILE

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
	rm -rf vine-run-info
	exit 0
}

dispatch "$@"
