#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=vine.status

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1

	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import prometheus_client" || return 1

	return 0
}

prepare()
{
	rm -f $STATUS_FILE

	return 0
}

run()
{
	${CCTOOLS_PYTHON_TEST_EXEC} vine_python_prometheus.py
	echo $? > $STATUS_FILE

	status=$(cat $STATUS_FILE)
	if [ $status -ne 0 ]
	then
		logfile=$(latest_vine_debug_log)
		if [ -f ${logfile} ]
		then
			echo "manager log:"
			cat ${logfile}
		fi
		exit 1
	fi

	exit 0
}

clean()
{
	rm -f $STATUS_FILE
	rm -rf vine-run-info

	exit 0
}


dispatch "$@"
