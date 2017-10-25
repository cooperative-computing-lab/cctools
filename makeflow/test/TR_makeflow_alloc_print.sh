#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd alloc
	../../src/makeflow alloc.mf --storage-print alloc.out
	if [ $? -eq 0 ]; then
		grep "Base" alloc.out > alloc.test
		exec diff -w alloc_print.expected alloc.test
	else
		exit 1
	fi
}

clean()
{
	cd alloc && ../../src/makeflow -c alloc.mf && rm -f alloc.out && rm -f alloc.test  && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
