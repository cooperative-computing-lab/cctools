#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

ticket=my.ticket

perl=${CCTOOLS_PERL}

check_needed()
{
	[ -n "${perl}" ] || return 1
	command -v openssl >/dev/null 2>&1
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

	PERL5LIB="$(pwd)/../src/bindings/perl" ${perl} ../src/bindings/perl/chirp_perl_example.pl $hostport $ticket

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
