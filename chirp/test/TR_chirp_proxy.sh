#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
p="./hostport.proxy.$PPID"

prepare()
{
	chirp_start local
	echo "$hostport" > "$c"
	chirp_start "chirp://$hostport/"
	echo "$hostport" > "$p"
	return 0
}

run()
{
	if ! [ -s "$c" -a -s "$p" ]; then
		return 0
	fi
	proxy=$(cat "$p")

	../src/chirp "$proxy" mkdir /data
	../src/chirp "$proxy" put /dev/stdin /data/foo <<EOF
foo bar
EOF
	[ "$(../src/chirp "$proxy" cat /data/foo)" = 'foo bar' ]

	../src/chirp_benchmark "$proxy" bench 2 2 2

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$p"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
