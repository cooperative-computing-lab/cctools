#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT=test.wmaster.input	
TEST_OUTPUT=test.wmaster.output	
PORT_FILE=wfm.port
PIDMASTER_FILE=wfm.pid
PIDWORKER_FILE=worker.pid

prepare()
{
	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT
	rm -f $PORT_FILE
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE

	./gen_ints_wfm.sh $TEST_INPUT 10

	../src/wavefront_master -Z $PORT_FILE ./sum_wfm.sh 10 10 $TEST_INPUT $TEST_OUTPUT &
	pid=$!
	echo $pid > $PIDMASTER_FILE

	#wait at most five seconds for master to come alive.
	wait_for_file_creation $PORT_FILE 5

	exit 0
}

run()
{
	answer=1854882
	port=`cat $PORT_FILE`

	../../work_queue/src/work_queue_worker -t60 localhost $port &
	pid=$!
	echo $pid > $PIDWORKER_FILE

	#Wait for all the output to be written to the file. We wait until last
	#modification to $TEST_OUTPUT is at least 3 second ago.
	wait_for_file_creation $TEST_OUTPUT 5
	wait_for_file_modification $TEST_OUTPUT 3

	value=`sed -n 's/^9 9 \([[:digit:]]*\)/\1/p' $TEST_OUTPUT`

	if [ -z $value ]
	then
		exit 1
	else
		exit $(($answer-$value))
	fi
}

clean()
{
	[ -f $PIDWORKER_FILE ] && /bin/kill -9 `cat $PIDWORKER_FILE`
	[ -f $PIDMASTER_FILE ] && /bin/kill -9 `cat $PIDMASTER_FILE`

	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT
	rm -f $PORT_FILE
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE

    exit 0
}

dispatch $@
