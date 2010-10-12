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
    ../../dttools/src/worker localhost 9091 &
    workerpid=$!

    echo $workerpid >| worker.pid
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
