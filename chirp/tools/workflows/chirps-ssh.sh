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
for i in 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26; do
    CONFUGA_NODE_LIST="${CONFUGA_NODE_LIST},chirp://disc${i}.crc.nd.edu:${PORT}/${CONFUGA_ROOT}"
    CONFUGA_NODE_NUKE="${CONFUGA_NODE_NUKE} ${CHIRP} -d chirp disc${i}.crc.nd.edu:${PORT} rm /${CONFUGA_ROOT};"
    ssh -o PreferredAuthentications=publickey "${USERNAME}@disc${i}.crc.nd.edu" "killall -u ${USERNAME} chirp_server"
    screen -t disc${i}.crc.nd.edu ssh -o PreferredAuthentications=publickey "${USERNAME}@disc$i.crc.nd.edu" "/bin/sh -c '"'CHIRP_ROOT='"$CHIRP_ROOT"'; rm -rf "$CHIRP_ROOT"; mkdir -p "$CHIRP_ROOT"; '"$CHIRP_SERVER"' --parent-death --auth=hostname --auth=unix --auth=ticket --challenge-dir='"$RENDEZVOUS"' --transient="$CHIRP_ROOT"/chirp.transient --root="$CHIRP_ROOT"/chirp.root --port='"$PORT"' --catalog-update=10s --debug=all --debug-file=:stdout --debug-rotate-max=0 --jobs --job-concurrency=100 --project-name='"$PROJECT"' --idle-clients=30m'"'"
done
