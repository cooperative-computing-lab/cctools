#!/bin/sh

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR
import_config_val CCTOOLS_OPSYS

export PATH=$(pwd)/../src:$(pwd)/../../batch_job/src:$PATH
export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

check_needed()
{

	
	# disable for now, as the conda test fails
	return 1

	[ "${CCTOOLS_OPSYS}" = LINUX ] || return 1

	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1

	return 0
}

prepare()
{
	exit 0
}

run()
{
	base=../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}
	${CCTOOLS_PYTHON_TEST_EXEC} ${base}/example_simple_limit.py
}

clean()
{
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
