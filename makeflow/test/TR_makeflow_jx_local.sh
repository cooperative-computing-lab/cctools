#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	clean $@
}

run()
{
	# slurm shouldn't be set up here, so this should only run
	# if local_job works
	cd jx && ../../src/makeflow -T slurm --jx local.jx && exit 0
	exit 1
}

clean()
{
	cd jx && ../../src/makeflow --jx -c local.jx && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
