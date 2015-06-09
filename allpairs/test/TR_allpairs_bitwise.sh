#!/bin/sh

. ../../dttools/test/test_runner_common.sh

TEST_INPUT=set.list
TEST_OUTPUT=master.out
PORT_FILE=worker.port

export PATH=.:../src:../../work_queue/src:$PATH

cleanfiles()
{
	rm -f $TEST_OUTPUT
	rm -f $PORT_FILE
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
	allpairs_master -x 1 -y 1 --output-file $TEST_OUTPUT -Z $PORT_FILE $TEST_INPUT $TEST_INPUT BITWISE &

	echo "waiting for $PORT_FILE to be created"
	wait_for_file_creation $PORT_FILE 5

	echo "starting worker"
	work_queue_worker localhost `cat $PORT_FILE` --timeout 2

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
	cleanfiles
	exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
