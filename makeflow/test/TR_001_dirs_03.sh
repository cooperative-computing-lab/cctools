#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    clean $@
}

run()
{
    exec ../src/makeflow -d all -T wq -p 9091 dirs/testcase.subdir.${i}.makeflow
}

clean()
{
    exec ../src/makeflow -c dirs/testcase.subdir.${i}.makeflow
}

i=03

dispatch $@
