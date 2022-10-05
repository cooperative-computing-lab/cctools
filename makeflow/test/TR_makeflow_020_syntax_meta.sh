#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd syntax; ../../src/makeflow meta.makeflow && exit 0
	exit 1
}

clean()
{
	cd syntax; ../../src/makeflow -c meta.makeflow && rm *.makeflowlog && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
