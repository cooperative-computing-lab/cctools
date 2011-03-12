#!/bin/sh

. ../../dttools/src/test_runner.common.sh

pidfile=worker.pid
portfile=worker.port

prepare()
{
    workerport=`find_free_port`
    ../../dttools/src/worker -d all localhost $workerport &
    workerpid=$!
    echo $workerpid > $pidfile
    echo $workerport > $portfile
    ln -s ../src/allpairs_multicore .
    exit 0
}

run()
{
    exec ../src/allpairs_master -d all -p `cat $portfile` set.list set.list ./lc_compare.sh
}

clean()
{
    kill -9 `cat $pidfile`
    rm -f $pidfile $portfile
    rm -f allpairs_multicore
    exit 0
}

dispatch $@
