#!/bin/bash

N=25
CCTOOLS_INSTALL="${CCTOOLS_INSTALL:-${HOME}/cctools-install/bin}"
RENDEZVOUS="${RENDEZVOUS:-${HOME}/rendezvous/}"
USERNAME="${USERNAME:-$(whoami)}"
PROJECT=-"${USERNAME}-confuga"

CHIRP="${CHIRP:-${CCTOOLS_INSTALL}/chirp}"
CHIRP_SERVER="${CHIRP_SERVER:-${CCTOOLS_INSTALL}/chirp_server}"

condor_rm "$USERNAME"
sleep 60
condor_submit <<EOF
arguments = --parent-death --auth=hostname --auth=unix --auth=ticket --challenge-dir=${RENDEZVOUS} --root=./root --catalog-update=10s --debug=all --debug-file=:stderr --debug-rotate-max=0 --jobs --job-concurrency=100 --project-name=${PROJECT} --port=0 --idle-clients=30m
environment = "TCP_LOW_PORT=9000 TCP_HIGH_PORT=9999"
error = chirp.\$(CLUSTER).\$(PROCESS).err
executable = ${CHIRP_SERVER}
log = chirp.\$(CLUSTER).log
output = chirp.\$(CLUSTER).\$(PROCESS).out
rank = 1/TotalSlotCpus
request_cpus = TotalSlotCpus
request_disk = 250000000
request_memory = 2000
should_transfer_files = yes
stream_error = True
stream_output = True
universe = vanilla
when_to_transfer_output = on_exit

queue $N
EOF

CONFUGA_NODE_LIST=""
while sleep 1; do
	CONFUGA_NODE_LIST=$("${CCTOOLS_INSTALL}/chirp_status" --server-lastheardfrom=60s --server-project="$PROJECT" --server-space=1G --verbose | tr $'\n' ' ' | sed 's/  /\n/g' | grep -E --only-matching 'url [^[:space:]]+' | sed 's/^url /,/g')
	if [ "$(echo "$CONFUGA_NODE_LIST" | wc -l)" -ge "$N" ]; then
		break;
	fi
done
export CONFUGA_NODE_LIST

# vim: set noexpandtab tabstop=4:
