#!/bin/sh

set -x

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

ticket=my.ticket

import_config_val CCTOOLS_PYTHON_TEST_EXEC
import_config_val CCTOOLS_PYTHON_TEST_DIR

export PYTHONPATH=$(pwd)/../../test_support/python_modules/${CCTOOLS_PYTHON_TEST_DIR}:$PYTHONPATH

check_needed()
{
	[ -n "${CCTOOLS_PYTHON_TEST_EXEC}" ] || return 1
}

prepare()
{
	[ -n "$CCTOOLS_PYTHON_TEST_EXEC" ] || return 1

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
 
	${CCTOOLS_PYTHON_TEST_EXEC} $(pwd)/../src/bindings/${CCTOOLS_PYTHON_TEST_DIR}/chirp_python_example.py $hostport $ticket


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
