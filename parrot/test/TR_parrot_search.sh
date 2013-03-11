#!/bin/sh

. ../../dttools/src/test_runner.common.sh

psearch="../src/parrot_run ../src/parrot_search"
out=output.txt
expected=expected.txt

prepare()
{
    touch $out
    make -C ../src
    exit 0
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
    [ -z "$failures" ] && echo "all tests passed" || echo $failures
}

clean()
{
    rm -f $out
    exit 0
}

dispatch $@
