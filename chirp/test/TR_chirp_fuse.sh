#!/bin/sh

set -x

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

CHIRP_DIR_FILE=chirp_fuse.dir
CHIRP_PID_FILE=chirp.pid
CHIRP_HOSTPORT_FILE=hostport.pid

import_config_val CCTOOLS_FUSE_AVAILABLE

check_needed()
{
	[ "${CCTOOLS_FUSE_AVAILABLE}" = yes ] || return 1
}

prepare()
{
	[ "${CCTOOLS_FUSE_AVAILABLE}" = yes ] || return 1

	chirp_start local --auth=ticket
	echo "$hostport" > "$CHIRP_HOSTPORT_FILE"

	return 0
}

run()
{
	if ! [ -s "$CHIRP_HOSTPORT_FILE" ]; then
		return 0
	fi

	hostport=$(cat "$CHIRP_HOSTPORT_FILE")

	mktemp -d /tmp/chirp.fuse.XXXXXX > "$CHIRP_DIR_FILE"
	CHIRP_DIR=$(cat $CHIRP_DIR_FILE)

	echo ../src/chirp_fuse -dall -aunix ${CHIRP_DIR}
	../src/chirp_fuse -dall -aunix -f ${CHIRP_DIR} &
	echo $! > ${CHIRP_PID_FILE}
	

	echo hello > ${CHIRP_DIR}/${hostport}/hello.txt

	out=$(cat ${CHIRP_DIR}/${hostport}/hello.txt)
	
	[ "$out" = hello ]
}

clean()
{
	chirp_clean

	CHIRP_DIR=$(cat "$CHIRP_DIR_FILE")
	fusermount -u $CHIRP_DIR

	kill -9 $(cat $CHIRP_PID_FILE)

	rm -rf ${CHIRP_DIR}
	rm -f "$c" "$CHIRP_DIR_FILE" "$CHIRP_HOSTPORT_FILE"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
