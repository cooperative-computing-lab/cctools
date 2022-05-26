#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	return 0
}

run()
{
	../src/jx_test < jx.input > jx.output
	diff jx.output jx.expected
	return $?
}

clean()
{
	rm -f jx.output
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
