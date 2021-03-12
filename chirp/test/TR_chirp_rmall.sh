#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

prepare()
{
	cat > default.acl <<EOF
unix:$(whoami) rwlda
unix:root rwlda
address:127.0.0.1 rlv(rwlda)
EOF
	DEFAULT_ACL=default.acl
	chirp_start local --auth=address
	echo "$hostport" > "$c"
	echo "$root" > "$cr"
	return 0
}

run()
{
	hostport=$(cat "$c")
	root=$(cat "$cr")

	chirp -a address "$hostport" mkdir /127.0.0.1
	chirp -a address "$hostport" put /etc/hosts /127.0.0.1/hosts
	chirp -a unix "$hostport" ls /127.0.0.1 && return 1
	chirp -a unix "$hostport" rm /127.0.0.1

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$cr" default.acl
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
