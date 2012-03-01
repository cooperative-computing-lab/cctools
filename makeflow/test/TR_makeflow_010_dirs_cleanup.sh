#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    exit 0
}

clean()
{
    kill -9 $(cat worker.pid)
    rm -f worker.pid
    rm -f worker.port
    rm -rf input
    exit 0
}

dispatch $@
