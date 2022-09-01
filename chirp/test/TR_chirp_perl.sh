#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

import_config_val CCTOOLS_PERL

export PERL5LIB="$(pwd)/../src/bindings/perl"

c="./hostport.$PPID"

ticket=my.ticket

check_needed()
{
	[ -n "${CCTOOLS_PERL}" ] || return 1
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

	PERL5LIB=$(pwd)/../src/bindings/perl ${CCTOOLS_PERL} ../src/bindings/perl/chirp_perl_example.pl $hostport $ticket

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
