#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT=set.list
TEST_OUTPUT=bitwise.output
PIDMASTER_FILE=allpairs.pid
PIDWORKER_FILE=worker.pid
PORT_FILE=WORKER.PORT

prepare()
{
	rm -f $TEST_OUTPUT
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE
	rm -f $PORT_FILE

    ../src/allpairs_master -x 1 -y 1 -o $TEST_OUTPUT -Z $PORT_FILE $TEST_INPUT $TEST_INPUT BITWISE &
    pid=$!
	echo $pid > $PIDMASTER_FILE

    ln -s ../src/allpairs_multicore .

	wait_for_file_creation $PORT_FILE 5
	exit 0
}

run()
{
    ../../dttools/src/work_queue_worker localhost `cat $PORT_FILE` &
    pid=$!
	echo $pid > $PIDWORKER_FILE

	wait_for_file_creation $TEST_OUTPUT 5
	wait_for_file_modification $TEST_OUTPUT 3

	#This is a horrible, horrible way to test things. Need to find a simple
	#example to actually test that the output is generated correctly.
	
	in_lines=`wc -l $TEST_INPUT | sed -n 's/\([[:digit:]]*\).*/\1/p'`
	[ -z $in_lines ] && exit 1
		
	in_lines=$(($in_lines * $in_lines))
	out_lines=`wc -l $TEST_OUTPUT | sed -n 's/\([[:digit:]]*\).*/\1/p'`

	exit $(($out_lines - $in_lines))

}

clean()
{
    kill -9 `cat $PIDMASTER_FILE`
    kill -9 `cat $PIDWORKER_FILE`
	
	rm -f $TEST_OUTPUT
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE
	rm -f $PORT_FILE
    rm -f allpairs_multicore

    exit 0
}

dispatch $@
