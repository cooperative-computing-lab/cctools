#!/bin/sh

. ../../dttools/test/test_runner_common.sh

python=${CCTOOLS_PYTHON_TEST_EXEC}
python_dir=${CCTOOLS_PYTHON_TEST_DIR}

check_needed()
{
		[ -n "${python}" ] || return 1

		exit 0
}

prepare()
{
	exit 0
}

run()
{
	base=../src/bindings/${python_dir}

	PYTHONPATH=${base} ${python} ${base}/example_simple_limit.py
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
