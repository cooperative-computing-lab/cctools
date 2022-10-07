#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR
PONCHO_PACKAGE_SERVERIZE=$(cd "$(dirname "$0")/../../poncho/src/"; pwd)/poncho_package_serverize

export PYTHONPATH=$(pwd)/../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

STATUS_FILE=wq.status
PORT_FILE=wq.port

run_wq_worker_coprocess()
{
	local port_file=$1
	shift
	local log=$1
	shift
	local coprocess=$1
	shift

	local timeout=15

	if [ -z "$log" ]; then
		log=worker.log
	fi

	echo "Waiting for manager to be ready."
	if wait_for_file_creation $port_file $timeout
	then
		echo "Master is ready on port `cat $port_file` "
	else
		echo "ERROR: Master failed to respond in $timeout seconds."
		exit 1
	fi
	echo "Running worker."
	if ! "$WORK_QUEUE_WORKER" --single-shot --timeout=10s --coprocess ${coprocess} --coprocess_cores 1 --coprocess_disk 1000 --coprocess_memory 1000 --debug=all --debug-file="$log" $* localhost $(cat "$port_file"); then
		echo "ERROR: could not start worker"
		exit 1
	fi
	echo "Worker completed."
	return 0
}

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1

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
    # ${PONCHO_PACKAGE_SERVERIZE} --src wq_remote_task.py --function add --function multiply --dest test_network_function.py

    # wait_for_file_creation test_network_function.py 5

    chmod +x network_function.py

	# send makeflow to the background, saving its exit status.
	# (${CCTOOLS_PYTHON_TEST_EXEC} wq_remote_task.py $PORT_FILE; echo $? > $STATUS_FILE) &

	# wait at most 5 seconds for wq to find a port.
	wait_for_file_creation $PORT_FILE 5

	run_wq_worker_coprocess $PORT_FILE worker.log network_function.py

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
