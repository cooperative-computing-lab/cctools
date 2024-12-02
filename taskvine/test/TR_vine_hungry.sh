#!/bin/sh
set -ex

. ../../dttools/test/test_runner_common.sh

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PATH=$(pwd)/../src/worker:$(pwd)/../../batch_job/src:$PATH
export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

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
	# send taskvine to the background, saving its exit status.
	exec ${CCTOOLS_PYTHON_TEST_EXEC} vine_python_hungry.py
}

clean()
{
	rm -rf vine-run-info

	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
