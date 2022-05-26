#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	exit 0
}

run()
{
	../src/makeflow_analyze -k syntax/directives.makeflow && exit 0
	exit 1
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
