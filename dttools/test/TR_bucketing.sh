#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
    return 0
}

run()
{
    ../src/bucketing_greedy_test > /dev/null
    val1=$?
    ../src/bucketing_exhaust_test > /dev/null
    val2=$?
    (test $val1 = 0) && (test $val2 = 0)
    return $?
}

clean()
{
    return 0
}

dispatch "$@"
