#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	return 0
}

run()
{
	../src/mq_wait_test
	return $?
}

clean()
{
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
