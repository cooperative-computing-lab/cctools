#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_hdfs_root="hdfs:///users/$USER/$0.$(hostname).$PPID"

prepare()
{
	set +e
	../src/chirp_server_hdfs --background --debug=all --debug-file="$chirp_debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$chirp_pid" --port-file="$chirp_port" --root="$chirp_hdfs_root"

	for ((i = 0; i < 30; i++)); do
		if [ -r "$chirp_pid" -a -r "$chirp_port" ]; then
			return 0
		fi
		sleep 1
	done
	# Not all platforms can run chirp_server_hdfs, this is reflected in a fatal error during backend startup
	echo "Failure to startup chirp_server_hdfs:"
	cat "$chirp_debug"
	return 0
}

run()
{
	set -e
	if ! [ -r "$chirp_pid" -a -r "$chirp_port" ]; then
		return 0
	fi
	../src/chirp -a unix localhost:`cat "$chirp_port"` mkdir /data
	../src/chirp -a unix localhost:`cat "$chirp_port"` put /dev/stdin /data/foo <<EOF
foo bar
EOF
	[ "$(../src/chirp -a unix localhost:`cat "$chirp_port"` cat /data/foo)" = 'foo bar' ]

	../src/chirp_benchmark localhost:`cat "$chirp_port"` bench 2 2 2
	../src/chirp -a unix localhost:`cat "$chirp_port"` rm /

	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill `cat "$chirp_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port"
}

dispatch $@

# vim: set noexpandtab tabstop=4:
