#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_root=chirp.root

chirp_proxy_debug=chirp.proxy.debug
chirp_proxy_pid=chirp.proxy.pid
chirp_proxy_port=chirp.proxy.port

prepare()
{
	../src/chirp_server --background --debug=all --debug-file="$chirp_debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$chirp_pid" --port-file="$chirp_port" --root="$chirp_root"

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	set -e

	../src/chirp_server --background --debug=all --debug-file="$chirp_proxy_debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$chirp_proxy_pid" --port-file="$chirp_proxy_port" --root=chirp://localhost:`cat "$chirp_port"`/

	wait_for_file_creation "$chirp_proxy_port" 5
	wait_for_file_creation "$chirp_proxy_pid" 5

	../src/chirp -a unix localhost:`cat "$chirp_proxy_port"` mkdir /data
	../src/chirp -a unix localhost:`cat "$chirp_proxy_port"` put /dev/stdin /data/foo <<EOF
foo bar
EOF
	cmp "$chirp_root"/data/foo <<EOF
foo bar
EOF
	[ "$(../src/chirp -a unix localhost:`cat "$chirp_proxy_port"` cat /data/foo)" = 'foo bar' ]

	../src/chirp_benchmark localhost:`cat "$chirp_proxy_port"` bench 2 2 2

	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill `cat "$chirp_pid"`
		/bin/kill `cat "$chirp_proxy_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port" "$chirp_root" "$chirp_proxy_debug" "$chirp_proxy_pid" "$chirp_proxy_port"
}

dispatch $@

# vim: set noexpandtab tabstop=4:
