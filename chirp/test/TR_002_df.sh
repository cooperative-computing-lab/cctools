#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid

prepare()
{
    ../src/chirp_server -p 9095 &
    pid=$!
    
    echo $pid > $PID_FILE
    exit 0
}

run()
{
    exec chirp localhost:9095 df -g 
}

clean()
{
    kill -9 $(cat $PID_FILE)
    rm -f $TEST_FILE
    rm -f $PID_FILE
    rm -f .__acl
    exit 0
}

dispatch $@
