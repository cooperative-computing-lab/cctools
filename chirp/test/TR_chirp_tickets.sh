#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

ticket=my.ticket

check_needed()
{
	command -v openssl >/dev/null 2>&1 || return 1
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

	chirp -a unix "$hostport" mkdir /data
	chirp -a unix "$hostport" put /dev/stdin /data/foo <<EOF
foo bar
EOF

	chirp -d all -a unix "$hostport" ticket_create -output "$ticket" -bits 1024 -duration 86400 -subject unix:`whoami` /data rwl

	chirp -d all -a ticket --tickets="$ticket" "$hostport" ls /data

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
