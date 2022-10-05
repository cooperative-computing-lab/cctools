#!/bin/sh
set -e

. ../../dttools/test/test_runner_common.sh

python=${CCTOOLS_PYTHON_TEST_EXEC}
python_dir=${CCTOOLS_PYTHON_TEST_DIR}

STATUS_FILE=wq.status
PORT_FILE=wq.port


check_needed()
{
	[ -n "${python}" ] || return 1
}

prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	return 0
}

run()
{
	# send wq to the background, saving its exit status.
	(PYTHONPATH=$(pwd)/../src/bindings/${python_dir} ${python} wq_test_tag.py $PORT_FILE; echo $? > $STATUS_FILE) &

	# wait at most 15 seconds for wq to find a port.
	wait_for_file_creation $PORT_FILE 15

	run_local_worker $PORT_FILE worker.log

	# wait for wq to exit.
	wait_for_file_creation $STATUS_FILE 15

	# retrieve wq exit status
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
	rm -f debug.log

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
