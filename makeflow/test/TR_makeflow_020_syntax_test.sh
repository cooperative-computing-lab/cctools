#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    ln ../src/makeflow ../src/makeflow_analyze
    exit 0
}

run()
{
    ../src/makeflow_analyze -k syntax/test.makeflow && exit 0
    exit 1
}

clean()
{
    rm -f ../src/makeflow_analyze
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
