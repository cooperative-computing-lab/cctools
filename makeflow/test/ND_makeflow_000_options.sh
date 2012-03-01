#!/bin/sh

. ../../dttools/src/test_runner.common.sh


prepare()
{
    exit 0
}

run()
{
    tmpdir=`mktemp -d`
    cp ../src/makeflow $tmpdir
    cp syntax/options.makeflow $tmpdir
    cd $tmpdir
    if ./makeflow -d all -T condor options.makeflow && rm -fr $tmpdir; then
    	exit 0
    else
	exit 1
    fi
}

clean()
{
    exit 0
}

dispatch $@
