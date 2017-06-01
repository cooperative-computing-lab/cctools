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
	cd json && ../../src/makeflow -T slurm --json local.json && exit 0
	exit 1
}

clean()
{
	cd json && ../../src/makeflow --json -c local.json && exit 0
	exit 1
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
