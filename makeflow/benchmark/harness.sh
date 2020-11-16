#!/bin/bash

# Runs a given test directory in $TMP. First, copy the contents of the test
# to a fresh temp directory. Then (in a subshell) cd there and run "./test.sh".
# Any additional filenames given will be copied out of the test dir into the
# current directory, then the test dir will be cleaned up.
#
# For example, suppose you have a test in "test1/" that will produce the files
# "output.txt" and "log.txt". The command would be
#     ./harness.sh test1/ output.txt log.txt
# After the test runs, the files "output.txt" and "log.txt" will be copied
# from the root of the test dir to the CWD.

if [ "$#" -lt 1 ]; then
	echo "usage: $0 <TESTDIR> [resource_output.summary]"
	exit 1
fi

# make sure to clean up the temp directory
unset TESTDIR
function cleanup {
	rm -rf "$TESTDIR"
}
trap cleanup EXIT
TESTDIR=$(mktemp -d)

cp -r "$1"/* "$TESTDIR"

( cd "$TESTDIR" && ./test.sh )

shift
while [ "$#" -gt 0 ]; do
	cp -r "$TESTDIR/$1" .
	shift
done

#python parsejson.py
