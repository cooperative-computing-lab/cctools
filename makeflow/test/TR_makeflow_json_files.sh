#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd json && ../../src/makeflow --json files.json && exit 0
	exit 1
}

clean()
{
	cd json && ../../src/makeflow --json -c files.json && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
