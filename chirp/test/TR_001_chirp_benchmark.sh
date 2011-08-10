#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid
PORT_FILE=chirp_server.port

prepare()
{
    ../src/chirp_server -Z $PORT_FILE &
    pid=$!

    # give the server a moment to generate the port file
    sleep 5

    if ps ux | awk '{print $2}' | grep "^$pid$"; then
	echo $port> $PORT_FILE
	echo $pid > $PID_FILE
	exit 0
    else
    	exit 1
    fi
}

run()
{
    port=`cat $PORT_FILE`
    exec ../src/chirp_benchmark localhost:$port $TEST_FILE 2 2 2
}

clean()
{
    kill -9 `cat $PID_FILE`
    rm -f $TEST_FILE
    rm -f $PID_FILE
    rm -f $PORT_FILE
    rm -f .__acl
    exit 0
}

dispatch $@
