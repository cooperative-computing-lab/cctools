#!/bin/sh

. ../../dttools/src/test_runner.common.sh

psearch="../src/parrot_run ../src/parrot_search"
expected=expected.txt

out=output.txt

prepare()
{
    rm -f $out
}

run()
{
    $psearch fixtures/a bar >> $out
    $psearch fixtures/a /bar >> $out
    $psearch fixtures/a c/bar >> $out
    $psearch fixtures/a /c/bar >> $out
    $psearch fixtures/a b/bar >> $out
    $psearch fixtures/a /b/bar >> $out
    $psearch fixtures/a b/foo >> $out
    $psearch fixtures/a /b/foo >> $out
    $psearch fixtures/a "/*/foo" >> $out
    $psearch fixtures/a "*/*r" >> $out

    failures=`diff --ignore-all-space $out $expected`
    if [ -z "$failures" ]; then
        echo "all tests passed"
        return 0
    else
        echo $failures
        return 1
    fi
}

clean()
{
    rm -f $out
}

dispatch $@
