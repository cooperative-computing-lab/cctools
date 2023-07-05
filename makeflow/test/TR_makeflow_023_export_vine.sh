#!/bin/sh

# Test that an environment variable can be set in the makeflow
# and then propagated all the way to a work queue task, where
# it is evaluated.  The backslashes ensure that the variables
# are evaluated by the task, not by makeflow.

. ../../dttools/test/test_runner_common.sh

# Just make a clean directory and enter it, so that everything
# is self-contained upon cleanup.

TEST_DIR=export.test.dir

prepare()
{
	mkdir -p $TEST_DIR
	cd $TEST_DIR
	cat > test.mf <<EOF
MAKEFLOW_INPUTS=""
MAKEFLOW_OUTPUTS=output.txt

export HELLO=hello makeflow
export GOODBYE
output.txt:
	echo \\\$HELLO \\\$GOODBYE > output.txt
EOF

	cat > expected.txt <<EOF
hello makeflow goodbye makeflow
EOF
	exit 0
}

run()
{
	cd $TEST_DIR

	export GOODBYE="goodbye makeflow"
	../../src/makeflow -d all -T vine -Z manager.port test.mf &

	wait_for_file_creation manager.port 5

	run_taskvine_worker manager.port worker.log

	require_identical_files output.txt expected.txt

	exit $?
}

clean()
{
	rm -rf ${TEST_DIR}
	rm -f worker.log
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
