#!/bin/sh

. ../../dttools/test/test_runner_common.sh

TEST_INPUT=set.list
TEST_OUTPUT=master.out
PORT_FILE=worker.port
WORKER_LOG=worker.log
MASTER_PID=master.pid
WORKER_PID=worker.pid
DONE_FILE=allpairs.done.$$

export PATH=.:../src:../../work_queue/src:$PATH

cleanfiles()
{
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
}

run()
{
	ln -s ../src/allpairs_multicore .

	echo "starting master"
	(allpairs_master -x 1 -y 1 --output-file $TEST_OUTPUT -Z $PORT_FILE $TEST_INPUT $TEST_INPUT BITWISE; touch $DONE_FILE) &
	echo $! > $MASTER_PID

	run_local_worker $PORT_FILE &
	echo $! > $WORKER_PID

	wait_for_file_creation $DONE_FILE 15

	echo "checking output"
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

	echo "output is good"
	exit 0

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
