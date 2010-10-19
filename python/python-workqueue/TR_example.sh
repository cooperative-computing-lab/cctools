#!/bin/sh

. ../../dttools/src/test_runner.common.sh

pidfile=worker.pid

prepare()
{
    ../../dttools/src/worker -d all localhost 9123 &
    workerpid=$!
    echo $workerpid > $pidfile
    exit 0
}

run()
{
    exec ${CCTOOLS_PYTHON} ./workqueue_example.py
}

clean()
{
    kill -9 $(cat $pidfile)
    rm -f $pidfile
    exit 0
}

dispatch $@
