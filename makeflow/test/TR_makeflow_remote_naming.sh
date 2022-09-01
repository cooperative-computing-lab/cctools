#!/bin/sh

# This test exercises the remote naming feature, in which a file
# can have a different name in the execution sandbox than it does
# in the makeflow working directory.

. ../../dttools/test/test_runner_common.sh

MAKE_FILE="remote_name.makeflow"
PORT_FILE="makeflow.port"
WORKER_LOG="worker.log"
STATUS_FILE="makeflow.status"

prepare()
{
	clean

cat > input.txt <<EOF
hello
EOF

cat > $MAKE_FILE <<EOF
MAKEFLOW_INPUTS=input.txt
MAKEFLOW_OUTPUTS=out.actual

out.1 -> out.txt: input.txt -> input.1
	cat input.1 > out.txt

out.2 -> out.txt:
	echo world > out.txt

out.actual: out.1 out.2
	cat out.1 out.2 > out.actual
EOF

cat > out.expected <<EOF
hello
world
EOF
}

run()
{
	../src/makeflow -d all -T wq -Z "$PORT_FILE" "$MAKE_FILE" &

	run_wq_worker "$PORT_FILE" "$WORKER_LOG"

	require_identical_files out.actual out.expected
}

clean()
{
	../src/makeflow -c $MAKE_FILE
	rm -f $MAKE_FILE $STATUS_FILE $PORT_FILE $WORKER_LOG out.1 out.2 out.actual out.expected input.txt
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
