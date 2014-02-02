#!/bin/sh

set -ex

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
chirp_hdfs_root="hdfs:///users/$USER/$0.$(hostname).$PPID"

prepare()
{
	if chirp_start "$chirp_hdfs_root"; then
		echo "$hostport" > "$c"
	fi
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	../src/chirp "$hostport" mkdir /data
	../src/chirp "$hostport" put /dev/stdin /data/foo <<EOF
foo bar
EOF
	[ "$(../src/chirp "$hostport" cat /data/foo)" = 'foo bar' ]

	../src/chirp_benchmark "$hostport" bench 2 2 2
	../src/chirp "$hostport" rm /

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
