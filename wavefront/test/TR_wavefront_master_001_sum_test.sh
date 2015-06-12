#!/bin/sh

. ../../dttools/test/test_runner_common.sh

PATH=../src:../../work_queue/src:$PATH

TEST_INPUT=test.wmaster.input
TEST_OUTPUT=test.wmaster.output
PORT_FILE=master.port
MASTER_PID=master.pid
MASTER_LOG=master.log
MASTER_OUTPUT=master.output

cleanfiles()
{
	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT
	rm -f $PORT_FILE
	rm -f $MASTER_PID
	rm -f $MASTER_LOG
	rm -f $MASTER_OUTPUT
}

prepare()
{
	cleanfiles
	./gen_ints_wfm.sh $TEST_INPUT 10
}

run()
{
	answer=1854882

	echo "starting wavefront master"
	wavefront_master -d all -o $MASTER_LOG -Z $PORT_FILE ./sum_wfm.sh 10 10 $TEST_INPUT $TEST_OUTPUT > $MASTER_OUTPUT &
	pid=$!
	echo $pid > $MASTER_PID

	echo "waiting for port file to be created"
	wait_for_file_creation $PORT_FILE 5

	echo "running worker"
	work_queue_worker --timeout 2 localhost `cat $PORT_FILE`

	echo "extracting result"
	value=`sed -n 's/^9 9 \([[:digit:]]*\)/\1/p' $TEST_OUTPUT`

	echo "computed value is $value"

	if [ X$value = X ]
	then
		echo "result is missing"
		exit 1
	elif [ $value = $answer ]
	then
		echo "result is correct"
		exit 0
	else
		echo "result is incorrect"
		exit 1
	fi
}

clean()
{
	if [ -f $MASTER_PID ]
	then
		kill -9 `cat $MASTER_PID`
	fi

	cleanfiles
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
