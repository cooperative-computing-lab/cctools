#!/bin/sh

. ../../dttools/test/test_runner_common.sh

export PATH=../src:$PATH

export CORES=4
export TASKS=20
export VALGRIND="valgrind --error-exitcode=1 --leak-check=full"

check_needed()
{
	if ! ${VALGRIND} --version > /dev/null 2>&1
	then
		exit 1
	fi

	# valgrind dumps core when testing on ubuntu16.04 and statically linked
	# workers on travis.
	if [ "${DOCKER_IMAGE}" = "cclnd/cctools-env:x86_64-ubuntu16.04" ]
	then
		exit 1
	fi
}


prepare()
{
	echo "nothing to do"
}

run()
{
	cat > master.script << EOF
submit 1 0 1 $TASKS
wait
quit
EOF

	echo "starting master"
	(${VALGRIND} --log-file=master.valgrind -- work_queue_test -d all -o master.log -Z master.port < master.script; echo $? > master.exitcode ) &

	echo "waiting for master to get ready"
	wait_for_file_creation master.port 5

	port=$(cat master.port)

	echo "starting worker"
	(${VALGRIND} --log-file=worker.valgrind -- work_queue_worker -d all -o worker.log localhost $port -b 1 --timeout 20 --cores $CORES --memory-threshold 10 --memory 50 --single-shot; echo $? > worker.exitcode)

	wait_for_file_creation master.exitcode 5
	wait_for_file_creation worker.exitcode 1

	echo "checking for valgrind errors"
	master=$(cat master.exitcode)
	worker=$(cat worker.exitcode)

	overall=0

	if [ "$master" != 0 ]
	then
		echo "valgrind found errors with the master."
		[ -f master.valgrind ] && cat master.valgrind && overall=1
	fi

	if [ "$worker" != 0 ]
	then
		echo "valgrind found errors with the worker"
		[ -f worker.valgrind ] && cat worker.valgrind && overall=1
	fi

	return ${overall}
}

clean()
{
	rm -f master.script master.log master.port master.exitcode master.valgrind worker.log worker.exitcode worker.valgrind output.* input.*
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
