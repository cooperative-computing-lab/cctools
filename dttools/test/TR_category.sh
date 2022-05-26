#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	return 0
}

run()
{
	value=$(../src/category_test disk-test.data | tail -n1)

	test "$value" = "max through: 1500"
	return $?
}

clean()
{
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
