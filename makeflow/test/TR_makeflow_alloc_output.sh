#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	cd alloc
	../../src/makeflow alloc.mf --local-cores 2 --storage-type 2 --storage-limit 5 
	# This should fail as the output only tracking doesn't do an adequate job
	if [ $? -eq 1 ]; then
		exit 0
	else
		exit 1
	fi
}

clean()
{
	cd alloc && ../../src/makeflow -c alloc.mf && rm -f alloc.out  && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
