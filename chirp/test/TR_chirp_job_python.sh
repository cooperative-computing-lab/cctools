#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

python=${CCTOOLS_PYTHON_TEST}

check_needed()
{
	[ -n "${python}" ] || return 1
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

	base=../src/bindings/python${CCTOOLS_PYTHON_TEST_VERSION}

	PYTHONPATH=../src/bindings/python${CCTOOLS_PYTHON_TEST_VERSION} ${python} ../src/bindings/python/chirp_jobs_python_example.py $hostport ../src/bindings/python/my_script.sh

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
