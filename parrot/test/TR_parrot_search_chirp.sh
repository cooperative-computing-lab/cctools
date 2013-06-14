#!/bin/sh

. ../../dttools/src/test_runner.common.sh

PSEARCH="../src/parrot_run ../src/parrot_search"
EXPECTED=expected.txt

cpid=cpid.txt
debug=debug.txt
out=output.txt
port=port.txt

prepare()
{
    ../../chirp/src/chirp_server -r ./fixtures/a -I 127.0.0.1 -Z $port -b -B $cpid -d all -o $debug
	rm -f "$out"
}

run()
{
    $PSEARCH /chirp/localhost:`cat $port`/ bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ /bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ c/bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ /c/bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ b/bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ /b/bar >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ b/foo >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ /b/foo >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ "/*/foo" >> $out
    $PSEARCH /chirp/localhost:`cat $port`/ "*/*r" >> $out

    noerr=`cat $out | grep -v error`
    rm -f $out
    echo "$noerr" > $out

    failures=`diff --ignore-all-space $out $EXPECTED`
    if [ -n "$failures" ]; then
        echo $failures
        return 1
    fi
}

clean()
{
    kill -9 `cat $cpid`
    rm -f $out $cpid $port $debug
}

dispatch $@
