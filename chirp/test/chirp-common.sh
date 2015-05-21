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
	acl=`mktemp ./chirp.acl.XXXXXX`
	debug=`mktemp ./chirp.debug.XXXXXX`
	pid=`mktemp ./chirp.pid.XXXXXX`
	port=`mktemp ./chirp.port.XXXXXX`
	transient=`mktemp -d ./chirp.transient.XXXXXX`
	if [ "$1" = local ]; then
		root=`mktemp -d ./chirp.root.XXXXXX`
		if [ "$(id -u)" -eq 0 ]; then
			verbose chown 9999 "$root"
		fi
	else
		root="$1"
	fi
	shift
	if [ "$(id -u)" -eq 0 ]; then
		cat >> "$acl" <<EOF
unix:root rwlda
EOF
		verbose chown 9999 "$acl" "$debug" "$pid" "$transient"
		chirp_server --advertise=localhost --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --default-acl="$acl" --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" --transient="$transient" --user=9999 "$@"
	else
		chirp_server --advertise=localhost --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" --transient="$transient" "$@"
	fi
	result=$?
	if [ "$result" -eq 0 ]; then
		i=0
		while [ $i -lt 10 ]; do
			if [ -s "$pid" -a -s "$port" ]; then
				hostport="127.0.0.1:$(cat "$port")"
				unset acl debug pid port result transient
				return 0
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
	for pid in ./chirp.pid.*; do
		pid=$(cat "$pid")
		echo kill $pid
		if ! kill $pid; then
			echo could not kill $pid
		fi
	done
	verbose rm -rf ./chirp.acl.* ./chirp.debug.* ./chirp.pid.* ./chirp.port.* ./chirp.transient.* ./chirp.root.*
	return 0
}

# vim: set noexpandtab tabstop=4:
