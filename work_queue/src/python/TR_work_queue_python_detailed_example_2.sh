#!/bin/sh

. ../../../dttools/test/test_runner_common.sh

export `grep CCTOOLS_PYTHON= ../../../config.mk`

prepare()
{
	exit 0
}

run()
{
	export PATH=../../../dttools/src:$PATH
	exec ${CCTOOLS_PYTHON} ./work_queue_detailed_example_2.py
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
