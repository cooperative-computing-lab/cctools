#!/bin/sh

. ../../dttools/src/test_runner.common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
    mkdir $test_dir
    cd $test_dir
    ln -sf ../../src/makeflow .
    ln -sf ../syntax/pattern.makeflow .
    exit 0
}

run()
{
    cd $test_dir
    ./makeflow -d all pattern.makeflow
    if [ -r pattern.md5sum -a -r stat.pattern ]; then
    	exit 0
    else
    	exit 1
    fi
}

clean()
{
    rm -fr $test_dir $test_output
    exit 0
}

dispatch $@
