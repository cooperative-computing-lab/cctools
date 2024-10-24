#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="../src/priority_queue_test"

prepare()
{
	return 0
}

run()
{
	exec "$exe"
}

clean()
{
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
