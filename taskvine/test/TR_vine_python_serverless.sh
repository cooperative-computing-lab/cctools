#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=vine.status
PORT_FILE=vine.port
TEST_FILE=vine_python_serverless.py
TEST_INPUT_FILE=${TEST_FILE}.input
TEST_OUTPUT_FILE=${TEST_FILE}.output

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1

	# Poncho currently requires ast.unparse to serialize the function,
	# which only became available in Python 3.9.  Some older platforms
	# (e.g. almalinux8) will not have this natively.
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "from ast import unparse" || return 1

	# In some limited build circumstances (e.g. macos build on github),
	# poncho doesn't work due to lack of conda-pack or cloudpickle
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import conda_pack" || return 1
	"${CCTOOLS_PYTHON_TEST_EXEC}" -c "import cloudpickle" || return 1

        return 0
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE
        rm -f $TEST_INPUT_FILE
        rm -f $TEST_OUTPUT_FILE
	return 0
}

run()
{
	( ${CCTOOLS_PYTHON_TEST_EXEC} ${TEST_FILE} $PORT_FILE; echo $? > $STATUS_FILE ) &

	# wait at most 15 seconds for vine to find a port.
	wait_for_file_creation $PORT_FILE 15

	# run an artificially large worker in order to test high concurreny for serverless
	run_taskvine_worker $PORT_FILE worker.log --cores 8 --memory 1000 --disk 1000

	# wait for vine to exit.
	wait_for_file_creation $STATUS_FILE 15

	# retrieve exit status
	status=$(cat $STATUS_FILE)
	if [ $status -ne 0 ]
	then
		# display log files in case of failure.
		logfile=$(latest_vine_debug_log)
		if [ -f ${logfile}  ]
		then
			echo "master log:"
			cat ${logfile}
		fi

		if [ -f worker.log  ]
		then
			echo "worker log:"
			cat worker.log
		fi

		exit 1
	fi

	exit 0
}

clean()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE
        rm -f $TEST_INPUT_FILE
        rm -f $TEST_OUTPUT_FILE
	rm -rf vine-run-info
        rm worker.log
	exit 0
}


dispatch "$@"
