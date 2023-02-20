#!/bin/sh

. ../../dttools/test/test_runner_common.sh

export PATH=../src/tools:../src/worker:$PATH

export CORES=4
export TASKS=20
export VALGRIND="valgrind --error-exitcode=1 --leak-check=full"

check_needed()
{
	if ! ${VALGRIND} --version > /dev/null 2>&1
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
	cat > manager.script << EOF
submit 1 0 1 $TASKS
wait
quit
EOF

	echo "starting manager"
	(${VALGRIND} --log-file=manager.valgrind -- vine_benchmark -Z manager.port < manager.script; echo $? > manager.exitcode ) &

	echo "waiting for manager to get ready"
	wait_for_file_creation manager.port 15

	port=$(cat manager.port)

	echo "starting worker"
	(${VALGRIND} --log-file=worker.valgrind -- vine_worker -d all -o worker.log localhost $port -b 1 --timeout 20 --cores $CORES --memory-threshold 10 --memory 50 --single-shot; echo $? > worker.exitcode)

	wait_for_file_creation manager.exitcode 15
	wait_for_file_creation worker.exitcode 5

	echo "checking for valgrind errors"
	manager=$(cat manager.exitcode)
	worker=$(cat worker.exitcode)

	overall=0

	if [ "$manager" != 0 ]
	then
		echo "valgrind found errors with the manager."
		[ -f manager.valgrind ] && cat manager.valgrind && overall=1
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
	rm -f manager.script vine_benchmark_info manager.port manager.exitcode manager.valgrind worker.log worker.exitcode worker.valgrind output.* input.*
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
