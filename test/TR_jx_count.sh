#!/bin/sh

. test_runner_common.sh

prepare()
{
	$CCTOOLS_COMPILE jx_count.c -o jx_count -ldttools -lm
}

run()
{
	./jx_count 4 jx_count.input
}

clean()
{
	rm -f jx_count
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
