#!/bin/sh

. ../../dttools/test/test_runner_common.sh

python=${CCTOOLS_PYTHON3}

check_needed()
{
		[ -f ../src/bindings/python3/_resource_monitor.so ] || return 1

		exit 0
}

prepare()
{
	[ -n "$python" ] || return 1

	exit 0
}

run()
{
	PYTHONPATH=$(pwd)/../src/bindings/python3 ${CCTOOLS_PYTHON3} ../src/bindings/python3/example_simple_limit.py
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
