#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT="./set.list"
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

    ln -s ../src/allpairs_multicore .

    (PATH=.:$PATH ../src/allpairs_master -x 1 -y 1 -o $TEST_OUTPUT -Z $PORT_FILE $TEST_INPUT $TEST_INPUT BITWISE )&

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

	wait_for_file_creation $TEST_OUTPUT 5
	wait_for_file_modification $TEST_OUTPUT 3

	in_files=`cat "$TEST_INPUT" | awk -F"/" '{print $3}'`
	howmany() { echo $#;}
	num_files=$(howmany $in_files)
	for i in $in_files; do
	  count=`awk '{print $1}' $TEST_OUTPUT | grep -c $i`
	  if [ $num_files != $count ]
	  then
	    exit 1
	  fi
	  count=`awk '{print $2}' $TEST_OUTPUT | grep -c $i`
	  if [ $num_files != $count ]
	  then
	    exit 1
	  fi
	done

	exit 0

}

clean()
{
    /bin/kill -9 `cat $PIDMASTER_FILE`
    /bin/kill -9 `cat $PIDWORKER_FILE`
	
	rm -f $TEST_OUTPUT
	rm -f $PIDMASTER_FILE
	rm -f $PIDWORKER_FILE
	rm -f $PORT_FILE
    rm -f allpairs_multicore

    exit 0
}

dispatch $@
