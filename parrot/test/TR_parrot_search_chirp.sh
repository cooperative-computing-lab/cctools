#!/bin/sh

. ../../dttools/src/test_runner.common.sh

EXPECTED=expected.txt

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
parrot_debug=parrot.debug
search_output=search.txt

psearch="../src/parrot_run -d all -o $parrot_debug ../src/parrot_search"

prepare()
{
    ../../chirp/src/chirp_server -r ./fixtures/a -I 127.0.0.1 -Z $chirp_port -b -B $chirp_pid -d all -o $chirp_debug
    rm -f "$search_output"
}

run()
{
    $psearch /chirp/localhost:`cat $chirp_port`/ bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ /bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ c/bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ /c/bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ b/bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ /b/bar >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ b/foo >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ /b/foo >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ "/*/foo" >> $search_output
    $psearch /chirp/localhost:`cat $chirp_port`/ "*/*r" >> $search_output

    noerr=`cat $search_output | grep -v error`
    rm -f $search_output
    echo "$noerr" > $search_output

    failures=`diff --ignore-all-space $search_output $EXPECTED`
    if [ -n "$failures" ]; then
        echo $failures
        return 1
    fi
}

clean()
{
    kill -9 `cat $chirp_pid`
    rm -f $chirp_debug $chirp_pid $chirp_port $parrot_debug $search_output
}

dispatch $@
