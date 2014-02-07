#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
	exit 0
}

run()
{
    cd syntax;

    if ! ../../src/makeflow_analyze -k typo.makeflow
    then
    	exit 1
    fi

    if ../../src/makeflow typo.makeflow
    then
    	exit 1
    else
    	exit 0
    fi
}

clean()
{
    cd syntax; ../../src/makeflow -c typo.makeflow && exit 0
    exit 1
}

dispatch $@

# vim: set noexpandtab tabstop=4:
