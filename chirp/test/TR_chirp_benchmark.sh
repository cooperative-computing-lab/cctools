#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

prepare()
{
	chirp_start local
	echo "$hostport" > "$c"
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	../src/chirp_benchmark "$hostport" foo 2 2 2

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
