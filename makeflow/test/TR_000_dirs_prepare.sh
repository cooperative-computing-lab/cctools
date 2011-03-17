#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    cd ../src/; make; cd -
    
    rm -rf input
    rm -rf mydir

    mkdir -p input
    echo world > input/hello

    # start a worker
    workerport=`find_free_port`
    ../../dttools/src/work_queue_worker localhost $workerport &
    workerpid=$!

    echo $workerpid  > worker.pid
    echo $workerport > worker.port
    exit 0
}

run()
{
    exit 0
}

clean()
{
    exit 0
}

dispatch $@
