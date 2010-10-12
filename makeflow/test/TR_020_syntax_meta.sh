#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    clean $@
}

run()
{
    cd syntax; ../../src/makeflow meta.makeflow && exit 0
    exit 1
}

clean()
{
    cd syntax; ../../src/makeflow -c meta.makeflow && exit 0
    exit 1
}

dispatch $@
