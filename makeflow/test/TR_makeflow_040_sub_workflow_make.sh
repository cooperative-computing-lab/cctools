#!/bin/sh

# Test recursive invocation of a makeflow.
# The top level makeflow creates a simple workflow, and then
# sends it to be executed completely at the worker side.

# Recursive makeflow requires that you have makeflow in your path.
export PATH=`pwd`/../../makeflow/src:$PATH

. ../../dttools/test/test_runner_common.sh

PORT_FILE="makeflow.port"
WORKER_LOG="worker.log"
STATUS_FILE="makeflow.status"

prepare()
{
clean

cat > input.txt <<EOF
hello
EOF

cat > toplevel.makeflow <<EOF
MAKEFLOW_INPUTS=input.txt
MAKEFLOW_OUTPUTS=out.actual

out.actual: input.txt
	MAKEFLOW sublevel.makeflow
EOF

cat > sublevel.makeflow <<EOF
MAKEFLOW_INPUTS=input.txt
MAKEFLOW_OUTPUTS=out.actual

out.1: input.txt
	cat input.txt > out.1

out.2:
	echo world > out.2

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
	../src/makeflow -Z "$PORT_FILE" toplevel.makeflow
	require_identical_files out.actual out.expected
}

clean()
{
	../src/makeflow -Z "$PORT_FILE" sublevel.makeflow -c
	../src/makeflow -Z "$PORT_FILE" toplevel.makeflow -c

	rm -f $MAKE_FILE $PORT_FILE $WORKER_LOG input.txt out.1 out.2 out.actual out.expected toplevel.makeflow sublevel.makeflow worker.log
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
