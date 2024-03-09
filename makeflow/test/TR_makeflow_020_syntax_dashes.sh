#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	exit 0
}

run()
{
	cd syntax && ../../src/makeflow dashes.makeflow
}

clean()
{
	cd syntax && ../../src/makeflow -c dashes.makeflow 
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
