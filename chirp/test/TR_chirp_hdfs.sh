#!/bin/sh

set -ex

. ../../dttools/src/test_runner.common.sh
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

	chirp "$hostport" mkdir /data
	chirp "$hostport" put /dev/stdin /data/foo <<EOF
foo bar
EOF
	[ "$(chirp "$hostport" cat /data/foo)" = 'foo bar' ]

	chirp_benchmark "$hostport" bench 1 1 1
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
