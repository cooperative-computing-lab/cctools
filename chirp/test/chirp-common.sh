chirp() {
	echo ../../chirp/src/chirp -d chirp "$@" >&2
	../../chirp/src/chirp -d chirp "$@"
}

chirp_benchmark() {
	echo ../../chirp/src/chirp_benchmark "$@" >&2
	../../chirp/src/chirp_benchmark "$@"
}

chirp_server() {
	echo ../../chirp/src/chirp "$@" >&2
	../../chirp/src/chirp_server "$@"
}

chirp_start() {
	debug=`mktemp ./chirp.debug.XXXXXX`
	pid=`mktemp ./chirp.pid.XXXXXX`
	port=`mktemp ./chirp.port.XXXXXX`
	transient=`mktemp -d ./chirp.transient.XXXXXX`
	if [ "$1" = local ]; then
		root=`mktemp -d ./chirp.root.XXXXXX`
	else
		root="$1"
	fi
	shift
	if chirp_server --advertise=localhost --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" --transient="$transient" "$@"; then
		i=0
		while [ $i -lt 10 ]; do
			if [ -s "$pid" -a -s "$port" ]; then
				hostport="127.0.0.1:$(cat "$port")"
				unset debug pid port transient
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
	unset debug pid port transient
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
	echo rm -rf ./chirp.debug.* ./chirp.pid.* ./chirp.port.* ./chirp.transient.* ./chirp.root.*
	rm -rf ./chirp.debug.* ./chirp.pid.* ./chirp.port.* ./chirp.transient.* ./chirp.root.*
	return 0
}

# vim: set noexpandtab tabstop=4:
