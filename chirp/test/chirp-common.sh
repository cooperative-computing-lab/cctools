CHIRP_SERVER=../src/chirp_server

chirp_start()
{
	debug=`mktemp ./chirp.debug.XXXXXX`
	pid=`mktemp ./chirp.pid.XXXXXX`
	port=`mktemp ./chirp.port.XXXXXX`
	if [ "$1" = local ]; then
		root=`mktemp -d ./chirp.root.XXXXXX`
	else
		root="$1"
		shift
	fi
	echo "$CHIRP_SERVER" --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" "$@"
	if "$CHIRP_SERVER" --auth=unix --background --debug=all --debug-file="$debug" --debug-rotate-max=0 --interface=127.0.0.1 --pid-file="$pid" --port-file="$port" --root="$root" "$@"; then
		for ((i = 0; i < 10; i++)); do
			if [ -s "$pid" -a -s "$port" ]; then
				hostport="127.0.0.1:$(cat "$port")"
				unset debug pid port root
				return 0
			fi
			sleep 1
		done
		echo "chirp_server did not start:"
	else
		echo "chirp_server failed with code: $?"
	fi
	touch "$debug"
	cat "$debug"
	unset debug pid port root
	return 1
}

chirp_clean()
{
	for pid in ./chirp.pid.*; do
		pid=$(cat "$pid")
		echo kill $pid
		if ! kill $pid; then
			echo could not kill $pid
		fi
	done
	echo rm -rf ./chirp.debug.* ./chirp.pid.* ./chirp.port.* ./chirp.root.*
	rm -rf ./chirp.debug.* ./chirp.pid.* ./chirp.port.* ./chirp.root.*
	return 0
}

# vim: set noexpandtab tabstop=4:
