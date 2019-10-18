#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"


check_needed()
{
	[ -f ../src/bindings/python/_CChirp.so ] || return 1
	python=$(cctools_python -n 3 2.7 2.6)
	[ -n "$python" ] || return 1
	${python} -c "import json; import Chirp"
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

	../src/bindings/python/chirp_jobs_python_example.py $hostport ../src/bindings/python/my_script.sh

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
