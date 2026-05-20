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
    ../src/bucketing_manager_test -greedy > /dev/null
    val3=$?
    ../src/bucketing_manager_test -exhaust > /dev/null
    val4=$?
    ../src/bucketing_base_test -det-greedy > /dev/null
    val5=$?
    ../src/bucketing_base_test -det-exhaust > /dev/null
    val6=$?
    ../src/bucketing_manager_test -det-greedy > /dev/null
    val7=$?
    ../src/bucketing_manager_test -det-exhaust > /dev/null
    val8=$?
    (test $val1 = 0) && (test $val2 = 0) && (test $val3 = 0) && (test $val4 = 0) && (test $val5 = 0) && (test $val6 = 0) && (test $val7 = 0) && (test $val8 = 0)
    return $?
}

clean()
{
    return 0
}

dispatch "$@"
