#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_root=chirp.root

ticket=my.ticket

prepare()
{
	../src/chirp_server --background --debug=all --debug-file="$chirp_debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$chirp_pid" --port-file="$chirp_port" --root="$chirp_root"

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	set -e

	../src/chirp -a unix localhost:`cat "$chirp_port"` mkdir /data
	../src/chirp -a unix localhost:`cat "$chirp_port"` put /dev/stdin /data/foo <<EOF
foo bar
EOF

	../src/chirp -d all -a unix localhost:`cat "$chirp_port"` ticket_create -output "$ticket" -bits 1024 -duration 86400 -subject unix:`whoami` /data rwl

	../src/chirp -d all -a ticket --tickets="$ticket" localhost:`cat "$chirp_port"` ls /data

	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill -9 `cat "$chirp_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port" "$chirp_root" "$ticket"
}

dispatch $@
