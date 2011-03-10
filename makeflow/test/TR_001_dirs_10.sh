#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    clean $@
}

run()
{
    exec ../src/makeflow -d all -T wq -p `cat worker.port` dirs/testcase.subdir.${i}.makeflow
}

clean()
{
    exec ../src/makeflow -c dirs/testcase.subdir.${i}.makeflow
}

i=10

dispatch $@
