#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR
PONCHO_PACKAGE_SERVERIZE=$(cd "$(dirname "$0")/../../poncho/src/"; pwd)/poncho_package_serverize

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=wq.status
PORT_FILE=wq.port

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

	return 0
}

run()
{
	${PONCHO_PACKAGE_SERVERIZE} --src wq_remote_task.py --function add --function multiply --function kwargs_test --function no_arguments_test --function exception_test --dest serverless_function.py --version work_queue
	wait_for_file_creation serverless_function.py 5

    chmod +x serverless_function.py

	# send makeflow to the background, saving its exit status.
	(${CCTOOLS_PYTHON_TEST_EXEC} wq_remote_task.py $PORT_FILE; echo $? > $STATUS_FILE) &

	# wait at most 5 seconds for wq to find a port.
	wait_for_file_creation $PORT_FILE 5

	coprocess="--coprocess serverless_function.py --coprocesses-total 1"
	coprocess_cores=2
	coprocess_memory=1000
	coprocess_disk=1000
	coprocess_gpus=0
	run_wq_worker $PORT_FILE worker.log 

	# wait for wq to exit.
	wait_for_file_creation $STATUS_FILE 5

	# retrieve makeflow exit status
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

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
