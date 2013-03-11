#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_server="../../chirp/src/chirp_server"
psearch="../src/parrot_run ../src/parrot_search"
out=output.txt
expected=expected.txt
cpid=cpid.txt
debug=debug.txt

prepare()
{
    $chirp_server -r "`pwd`/fixtures/a"  -p 9001 -d all -o $debug &
    echo $! > $cpid
    touch $out
    make -C ../src
    exit 0
}

run()
{
    $psearch /chirp/localhost:9001/ bar >> $out
    $psearch /chirp/localhost:9001/ /bar >> $out
    $psearch /chirp/localhost:9001/ c/bar >> $out
    $psearch /chirp/localhost:9001/ /c/bar >> $out
    $psearch /chirp/localhost:9001/ b/bar >> $out
    $psearch /chirp/localhost:9001/ /b/bar >> $out
    $psearch /chirp/localhost:9001/ b/foo >> $out
    $psearch /chirp/localhost:9001/ /b/foo >> $out
    $psearch /chirp/localhost:9001/ "/*/foo" >> $out
    $psearch /chirp/localhost:9001/ "*/*r" >> $out

    noerr=`cat $out | grep -v error`
    rm -f $out
    echo "$noerr" > $out

    failures=`diff --ignore-all-space $out $expected`
    [ -z "$failures" ] && echo "all tests passed" || echo $failures
}

clean()
{
    kill -9 `cat $cpid`
    rm -f $out $cpid
    exit 0
}

dispatch $@
