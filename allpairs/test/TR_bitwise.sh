#!/bin/sh

. ../../dttools/src/test_runner.common.sh

pidfile=worker.pid

prepare()
{
    ../../dttools/src/worker -d all localhost 9098 &
    workerpid=$!
    echo $workerpid > $pidfile
    ln -s ../src/allpairs_multicore .
    exit 0
}

run()
{
    exec ../src/allpairs_master -d all -p 9098 set.list set.list BITWISE
}

clean()
{
    kill -9 $(cat $pidfile)
    rm -f $pidfile
    rm -f allpairs_multicore
    exit 0
}

dispatch $@
