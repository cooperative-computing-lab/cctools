#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
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

	chirp "$proxy" mkdir /data
	chirp "$proxy" put /dev/stdin /data/foo <<EOF
foo bar
EOF
	[ "$(chirp "$proxy" cat /data/foo)" = 'foo bar' ]

	chirp_benchmark "$proxy" bench 10 10 0

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
