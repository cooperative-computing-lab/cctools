#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd jx && ../../src/makeflow --jx env.jx && exit 0
	exit 1
}

clean()
{
	cd jx && ../../src/makeflow --jx -c env.jx && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
