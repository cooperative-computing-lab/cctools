#!/bin/sh

. ../../dttools/test/test_runner_common.sh

TEST_INPUT=integer.list
TEST_OUTPUT_STEP=composite_step.output
TEST_OUTPUT=composite.output
TEST_TRUTH=composites.txt
PORT_FILE=worker.port
WORKER_LOG=worker.log
MASTER_PID=master.pid
WORKER_PID=worker.pid
DONE_FILE=allpairs.done.$$

export PATH=.:../src:../../work_queue/src:$PATH

cleanfiles()
{
	if [ -f $TEST_INPUT ]
	then
		xargs rm < $TEST_INPUT
	fi

	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT_STEP
	rm -f $TEST_OUTPUT
	rm -f $PORT_FILE
	rm -f $WORKER_LOG
	rm -f $MASTER_PID
	rm -f $WORKER_PID
	rm -f $DONE_FILE
	rm -f allpairs_multicore
}

prepare()
{
	cleanfiles
	./gen_ints.sh $TEST_INPUT 20
	ln -s ../src/allpairs_multicore .
}

run()
{
	echo "starting master"
	(allpairs_master -x 1 -y 1 --output-file $TEST_OUTPUT_STEP -Z $PORT_FILE $TEST_INPUT $TEST_INPUT ./divisible.sh -d all; touch $DONE_FILE) &
	echo $! > $MASTER_PID

	run_local_worker $PORT_FILE &
	echo $! > $WORKER_PID

	wait_for_file_creation $DONE_FILE 30

	echo "checking output"
	awk '$3 ~ /^0$/{print $1}' $TEST_OUTPUT_STEP | sort -n | uniq > $TEST_OUTPUT

	diff $TEST_TRUTH $TEST_OUTPUT
	exit $?
}

clean()
{
	kill -9 $(cat $MASTER_PID)
	kill -9 $(cat $WORKER_PID)

	cat $WORKER_LOG 1>&2

	cleanfiles
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
