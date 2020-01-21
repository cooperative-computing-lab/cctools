#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
	exit 0
}

run()
{
    cd syntax
    if ../../src/makeflow_analyze -k quotes.02.makeflow
    then
        echo "Open double quotes should have failed syntax check."
        exit 1
    fi

    cd syntax
    if ../../src/makeflow_analyze -k quotes.03.makeflow
    then
        echo "Open single quotes should have failed syntax check."
        exit 1
    fi

	exit 0
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
