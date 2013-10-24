#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    ln ../src/makeflow ../src/makeflow_util
    exit 0
}

run()
{
    ../src/makeflow_util -k syntax/test.makeflow && exit 0
    exit 1
}

clean()
{
    rm -f ../src/makeflow_util
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
