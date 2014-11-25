#!/bin/sh

set -ex

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

me=$(whoami)

if [ -z "$me" ]; then
	echo "I don't know who I am!" >&2
	exit 1
fi

c="./hostport.$PPID"
chirp_hdfs_root="hdfs:///users/${me}/.chirp.test/$0.$(hostname).$PPID"

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

	chirp_benchmark "$hostport" bench 10 10 0
	chirp "$hostport" rm /

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
