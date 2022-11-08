#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
    return 0
}

run()
{
    val1=$(../src/bucketing_greedy_test)
    val2=$(../src/bucketing_exhaust_test)
    test "$val1" = "0" && "$val2" = "0"
    return $?
}

clean()
{
    return 0
}

dispatch "$@"
