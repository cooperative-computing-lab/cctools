#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

ticket=my.ticket

check_needed()
{
	[ -f ../src/python/_CChirp.so ] || return 1
	python=$(cctools_python -n 3 2.7 2.6)
	[ -n "$python" ] || return 1
	${python} -c "import json; import Chirp"
}

prepare()
{
	chirp_start local --auth=ticket
	echo "$hostport" > "$c"
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	chirp -d all -a unix "$hostport" ticket_create -output "$ticket" -bits 1024 -duration 86400 -subject unix:`whoami` / write

	../src/bindings/python/chirp_python_example.py $hostport $ticket

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$ticket"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
