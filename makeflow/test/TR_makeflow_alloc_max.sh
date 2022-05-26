#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd alloc && ../../src/makeflow alloc.mf --storage-type 0 --storage-limit 12 && exit 0
	exit 1
}

clean()
{
	cd alloc && ../../src/makeflow -c alloc.mf && rm -f alloc.out && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
