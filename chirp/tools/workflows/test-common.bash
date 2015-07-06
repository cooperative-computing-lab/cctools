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

. "$(dirname "$0")/chirps-ssh.sh"

# vim: set noexpandtab tabstop=4:
