#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	exit 0
}

run()
{
	if ../src/makeflow_analyze -k syntax/directives_unsupported.makeflow
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

dispatch "$@"

# vim: set noexpandtab tabstop=4:
