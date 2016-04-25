#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
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

	chirp "$hostport" <<EOF
mkdir foo
mkdir foo/bar
ln -s ../foo foo/baz
ln -s ../../ /baz
stat baz
stat /
stat /baz/foo
stat /baz/foo/bar
stat /baz/foo/baz
EOF
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
