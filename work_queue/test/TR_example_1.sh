#!/bin/sh

. ../../../dttools/src/test_runner.common.sh

export `grep CCTOOLS_PYTHON= ../../../Makefile.config`

prepare()
{
    exit 0
}

run()
{
    export PATH=../../../dttools/src:$PATH
    exec ${CCTOOLS_PYTHON} ./work_queue_example_1.py
}

clean()
{
    exit 0
}

dispatch $@
