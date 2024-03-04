#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	return 0
}

run()
{
	../src/hash_table_offset_test
}

clean()
{
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
