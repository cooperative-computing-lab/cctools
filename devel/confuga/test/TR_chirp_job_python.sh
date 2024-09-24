#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
}

prepare()
{
	chirp_start local --auth=unix --jobs --job-concurrency=2
	echo "$hostport" > "$c"
	echo "$root" > "$cr"
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	base=$(pwd)/../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}

	${CCTOOLS_PYTHON_TEST_EXEC} ${base}/chirp_jobs_python_example.py $hostport ${base}/my_script.sh

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$cr"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
