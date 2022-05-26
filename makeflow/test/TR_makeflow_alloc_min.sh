#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd alloc
	../../src/makeflow alloc.mf --storage-type 1 --storage-limit 5 
	if [ $? -eq 0 ]; then
		exec diff -w alloc_min.expected alloc.out
	else
		exit 1
	fi
}

clean()
{
	cd alloc && ../../src/makeflow -c alloc.mf && rm -f alloc.out && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
