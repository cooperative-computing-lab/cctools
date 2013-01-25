#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid
PORT_FILE=chirp_server.port

prepare()
{
	rm -f $PORT_FILE	
	../src/chirp_server -Z $PORT_FILE &
	pid=$!
	echo $pid > $PID_FILE

	wait_for_file_creation $PORT_FILE 5

	exit 0
}

run()
{
    port=`cat $PORT_FILE`
    exec ../src/chirp_benchmark localhost:$port $TEST_FILE 2 2 2
}

clean()
{
	if [ -f $PID_FILE ]
	then
		kill -9 `cat $PID_FILE`
	fi

    rm -f $TEST_FILE
    rm -f $PID_FILE
    rm -f $PORT_FILE
    rm -f .__acl
    exit 0
}

dispatch $@
