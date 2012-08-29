#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    cd syntax
    
    if ../../src/makeflow -k variable_issues.makeflow
    then
    	exit 1
    else
	exit 0
    fi
}

clean()
{
    exit 0
}

dispatch $@
