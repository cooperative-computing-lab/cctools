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

prepare()
{
	# Darwin's tr is criminally stupid. It apparently ignores SIGPIPE for the
	# output of this pipeline. So we limit its input using dd:
	local guid="$(dd if=/dev/urandom bs=1024 count=1 | tr -d -c 'a-zA-Z0-9' | head -c 20)"
	if chirp_start "hdfs:///users/${me}/.chirp.test/${0}.${guid}"; then
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
