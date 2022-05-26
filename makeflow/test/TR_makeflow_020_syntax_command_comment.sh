#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	exit 0
}

run()
{
	cd syntax; ../../src/makeflow_analyze -k command_comment.makeflow && exit 0
	exit 1
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
