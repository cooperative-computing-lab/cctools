#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
    mkdir $test_dir
    cd $test_dir
    ln -s ../../src/makeflow .
    ln -s ../syntax/recursive.makeflow Makeflow
    ln -s ../syntax/options.makeflow .
    ln -s ../syntax/test.makeflow .
    exit 0
}

run()
{
    cd $test_dir
    if ./makeflow; then
    	exec makeflow -c
    else
    	exit 1
    fi
}

clean()
{
    rm -fr $test_dir
    exit 0
}

dispatch $@
