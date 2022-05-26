#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe=../src/parrot_test_dir

prepare()
{
	return 0
}

run()
{
	set -e
	./"$exe"
	parrot -- ./"$exe"
	return $?
}

clean()
{
	rm -rf foo
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
