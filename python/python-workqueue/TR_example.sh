#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    exec ${CCTOOLS_PYTHON} ./workqueue_example.py
}

clean()
{
    exit 0
}

dispatch $@
