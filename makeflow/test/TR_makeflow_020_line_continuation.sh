#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    clean $@
}

run()
{
    cd syntax && ../../src/makeflow line_continuation.makeflow && exit 0
    exit 1
}

clean()
{
    cd syntax; ../../src/makeflow -c line_continuation.makeflow && exit 0
    exit 1
}

dispatch $@

# vim: set noexpandtab tabstop=4:
