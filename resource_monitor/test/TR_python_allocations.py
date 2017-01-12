#!/bin/sh

. ../../dttools/test/test_runner_common.sh

export $(grep CCTOOLS_PYTHON2= ../../config.mk)

check_needed()
{
		[ -f ../src/python/_cResourceMonitor.so ]     || return 1

		export PYTHONPATH=$(pwd)/../src/python
		${CCTOOLS_PYTHON2} -c "import ResourceMonitor" || return 1

		exit 0
}


prepare()
{
		exit 0
}

run()
{
	export PYTHONPATH=$(pwd)/../src/python
	${CCTOOLS_PYTHON2} ../src/python/rmonitor_allocations_example.py
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
