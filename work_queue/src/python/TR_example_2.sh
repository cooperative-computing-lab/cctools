#!/bin/sh

. ../../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    export PATH=../../../dttools/src:$PATH
    exec ./work_queue_example_2.py
}

clean()
{
    exit 0
}

dispatch $@
