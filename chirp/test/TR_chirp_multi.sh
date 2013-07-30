#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_1_debug=chirp.1.debug
chirp_1_pid=chirp.1.pid
chirp_1_port=chirp.1.port
chirp_1_root=chirp.1.root

chirp_2_debug=chirp.2.debug
chirp_2_pid=chirp.2.pid
chirp_2_port=chirp.2.port
chirp_2_root=chirp.2.root

VOLUME="tank"
VOLUME_KEY="b63ae12fe3c8708ecae4d3cba504f5705af1440e" # `echo -n "$VOLUME" | sha1sum | awk '{print $1}'`

prepare()
{
	../src/chirp_server -r "$chirp_1_root" -I 127.0.0.1 -Z "$chirp_1_port" -b -B "$chirp_1_pid" -d all -o "$chirp_1_debug"
	../src/chirp_server -r "$chirp_2_root" -I 127.0.0.1 -Z "$chirp_2_port" -b -B "$chirp_2_pid" -d all -o "$chirp_2_debug"

	wait_for_file_creation "$chirp_1_port" 5
	wait_for_file_creation "$chirp_1_pid" 5
	wait_for_file_creation "$chirp_2_port" 5
	wait_for_file_creation "$chirp_2_pid" 5
}

run()
{
	set -e

	../src/chirp localhost:`cat "$chirp_1_port"` mkdir "$VOLUME"
	../src/chirp localhost:`cat "$chirp_1_port"` mkdir "$VOLUME"/root
	../src/chirp localhost:`cat "$chirp_1_port"` put /dev/stdin "$VOLUME"/hosts <<EOF
127.0.0.1:`cat $chirp_1_port`
127.0.0.1:`cat $chirp_2_port`
EOF
	../src/chirp localhost:`cat "$chirp_1_port"` put /dev/stdin "$VOLUME"/key <<EOF
$VOLUME_KEY
EOF

	# We can only test the multi interface through parrot...
	if [ -x ../../parrot/src/parrot_run ]; then
		../../parrot/src/parrot_run ls -l /multi/127.0.0.1:`cat "$chirp_1_port"`@"$VOLUME"/
		../../parrot/src/parrot_run df -h /multi/127.0.0.1:`cat "$chirp_1_port"`@"$VOLUME"/
		../../parrot/src/parrot_run sh -c 'echo 1 > /multi/127.0.0.1:'`cat "$chirp_1_port"`'@'"$VOLUME"'/foo'
		../../parrot/src/parrot_run sh -c 'echo 2 > /multi/127.0.0.1:'`cat "$chirp_1_port"`'@'"$VOLUME"'/bar'
	fi

	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_1_pid" ]; then
		/bin/kill -9 `cat "$chirp_1_pid"`
	fi
	if [ -r "$chirp_2_pid" ]; then
		/bin/kill -9 `cat "$chirp_2_pid"`
	fi

	rm -rf "$chirp_1_debug" "$chirp_1_pid" "$chirp_1_port" "$chirp_1_root"
	rm -rf "$chirp_2_debug" "$chirp_2_pid" "$chirp_2_port" "$chirp_2_root"
}

dispatch $@
