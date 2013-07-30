#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_root=chirp.root

prepare()
{
	../src/chirp_server -r "$chirp_root" -I 127.0.0.1 -Z "$chirp_port" -b -B "$chirp_pid" -d all -o "$chirp_debug"

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	../src/chirp_benchmark localhost:`cat "$chirp_port"` foo 2 2 2
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill -9 `cat "$chirp_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port" "$chirp_root"
}

dispatch $@
