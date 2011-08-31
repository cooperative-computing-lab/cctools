#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    export PATH=../../dttools/src:$PATH
    exec ${CCTOOLS_PYTHON} ./workqueue_example.py
}

clean()
{
    exit 0
}

dispatch $@
