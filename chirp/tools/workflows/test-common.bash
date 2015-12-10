PORT=9123

CCTOOLS_INSTALL=/afs/nd.edu/user31/pdonnel3/cctools-install/bin/
CHIRP="${CCTOOLS_INSTALL}/chirp"
CHIRP_SERVER="${CCTOOLS_INSTALL}/chirp_server"
MAKEFLOW="${CCTOOLS_INSTALL}/makeflow"
PARROT="${CCTOOLS_INSTALL}/parrot_run"
WEAVER="${CCTOOLS_INSTALL}/weaver"

function run {
	printf '%s\n' "$*"
	time "$@"
}

function runbg {
	printf '%s &\n' "$*"
	"$@" &
}

function weaver {
	run env PATH="${CCTOOLS_INSTALL}:$PATH" PYTHONPATH="$(dirname "$WEAVER")/../lib/python2.6/site-packages/" "$WEAVER" "$@"
}

function testrun {
	local workflow="$1"
	local base="$2"
	local opts="$3"

	if [ -d "$base" ]; then
		echo "$base already exists; continuing"
		return
	fi
	run mkdir "$base" || exit 1

	eval "$CONFUGA_NODE_NUKE"

	echo "$CONFUGA_NODE_LIST" > "${base}/nodes.lst"
	runbg "$CHIRP_SERVER" --auth=unix --catalog-update=30s --challenge-dir="${base}/" --debug=all --debug-file="${base}/confuga.debug" --debug-rotate-max=0 --inherit-default-acl --interface=127.0.0.1 --jobs --job-concurrency=0 --pid-file="${base}/confuga.pid" --port="$PORT" --root="confuga://${base}/confuga.root?nodes=file:${base}/nodes.lst&auth=unix&${opts}" --superuser="unix:$(whoami)" --transient="${base}/confuga.transient" "$@"

	local namespace="$(basename "$workflow")"
	run "$CHIRP" --auth=unix --debug=chirp --debug-file="${base}/chirp.debug" --timeout=60m localhost:"$PORT" put "${workflow}/" "${namespace}"

	run cp "${workflow}/Makeflow" "${base}/Makeflow"
	run "$MAKEFLOW" --batch-type=chirp --debug=all --debug-file="${base}/makeflow.debug" --debug-rotate-max=0 --working-dir="chirp://localhost:${PORT}/" --wrapper=$'{\n {}\n} > stdout.%% 2> stderr.%%' --wrapper-output="${namespace}/stdout.%%" --wrapper-output="${namespace}/stderr.%%" "${base}/Makeflow"

	run "$PARROT" --chirp-auth=unix --timeout=60m -- cp -r "/chirp/localhost:${PORT}/.confuga/" "${base}/confuga-proc/" || true

	kill %1
	wait
	sleep 10
}

. "$(dirname "$0")/chirps-ssh.sh"

# vim: set noexpandtab tabstop=4:
