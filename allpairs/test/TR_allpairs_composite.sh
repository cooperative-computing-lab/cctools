#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT=integer.list
TEST_OUTPUT_STEP=composite_step.output
TEST_OUTPUT=composite.output
TEST_TRUTH=composites.txt
PIDMASTER_FILE=allpairs.pid
PIDWORKER_FILE=worker.pid
PORT_FILE=worker.port

prepare()
{
	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT_STEP
	rm -f $TEST_OUTPUT
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE
	rm -f $PORT_FILE

	./gen_ints.sh $TEST_INPUT 20

    ln -s ../src/allpairs_multicore .
    (PATH=.:$PATH ../src/allpairs_master -x 1 -y 1 -o $TEST_OUTPUT_STEP -Z $PORT_FILE $TEST_INPUT $TEST_INPUT ./divisible.sh )&

    pid=$!
	echo $pid > $PIDMASTER_FILE

	wait_for_file_creation $PORT_FILE 5
	exit 0
}

run()
{
    ../../work_queue/src/work_queue_worker localhost `cat $PORT_FILE` &
    pid=$!
	echo $pid > $PIDWORKER_FILE

	wait_for_file_creation $TEST_OUTPUT_STEP 5
	wait_for_file_modification $TEST_OUTPUT_STEP 3

	awk '$3 ~ /^0$/{print $1}' $TEST_OUTPUT_STEP | sort -n | uniq > $TEST_OUTPUT

	diff $TEST_TRUTH $TEST_OUTPUT
	exit $?
}

clean()
{
    /bin/kill -9 `cat $PIDMASTER_FILE`
    /bin/kill -9 `cat $PIDWORKER_FILE`

	xargs rm < $TEST_INPUT
	
	rm -f $TEST_INPUT
	rm -f $TEST_OUTPUT_STEP
	rm -f $TEST_OUTPUT
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE
	rm -f $PORT_FILE
    rm -f allpairs_multicore

    exit 0
}

dispatch $@
