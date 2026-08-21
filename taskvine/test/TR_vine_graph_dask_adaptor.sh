#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=vine_graph_dask_adaptor.status

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import dask" || return 1
	return 0
}

prepare()
{
	rm -f $STATUS_FILE
	return 0
}

run()
{
	${CCTOOLS_PYTHON_TEST_EXEC} vine_graph_dask_adaptor.py
	echo $? > $STATUS_FILE

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
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
