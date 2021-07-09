#!/bin/sh

. test_runner_common.sh

prepare()
{
	$CCTOOLS_COMPILE jx_test.c -o jx_test -ldttools -lm
}

run()
{
	./jx_test < jx.input > jx.output
	diff jx.output jx.expected
	return $?
}

clean()
{
	rm -f jx.output jx_test
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
