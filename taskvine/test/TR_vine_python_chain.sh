#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

# vine.Factory requires vine_factory and vine_worker in the PATH
export PATH=../src/worker:../../batch_job/src:$PATH

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
}

prepare()
{
	return 0
}

run()
{
	${CCTOOLS_PYTHON_TEST_EXEC} vine_python_chain.py
}

clean()
{
	rm -f infile
	exit 0
}


dispatch "$@"
