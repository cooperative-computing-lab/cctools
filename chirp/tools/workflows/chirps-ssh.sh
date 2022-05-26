#!/bin/bash

CCTOOLS_INSTALL="${CCTOOLS_INSTALL:-${HOME}/cctools-install/bin}"
RENDEZVOUS="${RENDEZVOUS:-${HOME}/rendezvous/}"
USERNAME="${USERNAME:-$(whoami)}"
PROJECT=-"${USERNAME}-confuga"

CHIRP_ROOT="/disk/d10/${USERNAME}"
CONFUGA_ROOT="/users/${USERNAME}/.confuga/"

if [ "$(whoami)" = pdonnel3 ]; then
	PORT=9122
elif [ "$(whoami)" = nhazekam ]; then
	PORT=9222
else
	PORT=9522
fi

CHIRP="${CHIRP:-${CCTOOLS_INSTALL}/chirp}"
CHIRP_SERVER="${CHIRP_SERVER:-${CCTOOLS_INSTALL}/chirp_server}"

CONFUGA_NODE_LIST=""
CONFUGA_NODE_NUKE=""
for ((i = 2; i <= 26; i++)); do
	host="$(printf 'disc%.2d.crc.nd.edu' "$i")"
	hostport="${host}:${PORT}"
	CONFUGA_NODE_LIST="${CONFUGA_NODE_LIST},chirp://${hostport}/${CONFUGA_ROOT}"
	CONFUGA_NODE_NUKE="${CONFUGA_NODE_NUKE} ${CHIRP} -d chirp ${hostport} rm /${CONFUGA_ROOT};"
	ssh -o PreferredAuthentications=publickey "${USERNAME}@${host}" "killall -u ${USERNAME} chirp_server"
	screen -t "${host}" ssh -o PreferredAuthentications=publickey "${USERNAME}@${host}" "/bin/sh -c '"'CHIRP_ROOT='"$CHIRP_ROOT"'; rm -rf "$CHIRP_ROOT"; mkdir -p "$CHIRP_ROOT"; '"$CHIRP_SERVER"' --parent-death --auth=hostname --auth=unix --auth=ticket --challenge-dir='"$RENDEZVOUS"' --transient="$CHIRP_ROOT"/chirp.transient --root="$CHIRP_ROOT"/chirp.root --port='"$PORT"' --catalog-update=10s --debug=all --debug-file=:stdout --debug-rotate-max=0 --jobs --job-concurrency=100 --project-name='"$PROJECT"' --idle-clients=30m'"'"
done

# vim: set noexpandtab tabstop=4:
