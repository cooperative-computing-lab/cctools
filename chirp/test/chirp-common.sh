verbose() {
	printf '%s\n' "$*" >&2
	"$@"
}

chirp() {
	verbose ../../chirp/src/chirp -d chirp "$@"
}

chirp_benchmark() {
	verbose ../../chirp/src/chirp_benchmark "$@"
}

chirp_server() {
	verbose ../../chirp/src/chirp_server "$@"
}

chirp_start() {
	if [ "$(id -u)" -eq 0 ]; then
		test_dir=`mktemp -d /tmp/chirp.test_dir.XXXXXX`
	else
		test_dir=`mktemp -d chirp.test_dir.XXXXXX`
	fi

	echo $test_dir

	if [ "$1" = local ]; then
		root="$test_dir/chirp.root"
		mkdir -p "$root"
	else
		root="$1"
	fi

	acl="$test_dir/chirp.acl"
	debug="$test_dir/chirp.debug"
	pid="$test_dir/chirp.pid"
	port="$test_dir/chirp.port"
	transient="$test_dir/chirp.transient"
	touch "$acl" "$debug" "$pid" "$port"
	mkdir -p "$transient"
	shift

	if [ "$(id -u)" -eq 0 ]; then
		echo "$acl"
		cat >> "$acl" <<EOF
unix:root rwlda
EOF
		verbose chown -R 9999 "$test_dir"
		chirp_server --advertise=localhost --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --default-acl="$acl" --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" --transient="$transient" --user=9999 "$@"
	else
		chirp_server --advertise=localhost --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" --transient="$transient" "$@"
	fi
	result=$?
	if [ "$result" -eq 0 ]; then
		i=0
		while [ $i -lt 10 ]; do
			if [ -s "$pid" ]; then
				if [ -s "$port" ]; then
					hostport="127.0.0.1:$(cat "$port")"
					unset acl debug pid port result transient
					return 0
				elif ! kill -s 0 "$(cat "$pid")"; then
					break;
				fi
			fi
			echo $i sleeping waiting for server to start
			sleep 1
			i=$(expr $i + 1)
		done
		echo "chirp_server did not start:"
	else
		echo "chirp_server failed with code: $?"
	fi
	touch "$debug"
	cat "$debug"
	unset acl debug pid port result transient
	return 1
}

chirp_clean() {
	set +e
	for pid in ./chirp.test_dir.*/chirp.pid /tmp/chirp.test_dir.*/chirp.pid; do
		if [ -e "$pid" ]
		then
			pid=$(cat "$pid")
			echo kill $pid
			if ! kill $pid; then
				echo could not kill $pid
			fi
		fi
	done
	verbose rm -rf ./chirp.test_dir.* /tmp/chirp.test_dir.*
	return 0
}

# vim: set noexpandtab tabstop=4:
