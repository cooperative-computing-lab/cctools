#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
    return 0
}

run()
{
    ../src/bucketing_base_test -greedy > /dev/null
    val1=$?
    ../src/bucketing_base_test -exhaust > /dev/null
    val2=$?
    (test $val1 = 0) && (test $val2 = 0)
    return $?
}

clean()
{
    return 0
}

dispatch "$@"
