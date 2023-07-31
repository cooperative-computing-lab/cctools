#!/bin/sh

# This test evaluates the ability of parrot to load and run an executable via chirp.
# To avoid complex dependencies, this is a simple static hello-world.


set -e

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh
. ../../chirp/test/chirp-common.sh

exe=../src/parrot_test_execve

prepare()
{
	chirp_start local
	cp ${exe} ${root}/hello
	${root}/hello > expected.txt

	echo "$hostport" > config.txt

	return 0
}

run()
{
	hostport=$(cat "config.txt")


	parrot --no-chirp-catalog --timeout=5 --work-dir="/chirp/${hostport}/" ./hello > output.txt

	if diff output.txt expected.txt
	then
		return 0
	else
		echo -n "Incorrect output: "
		cat output.txt
		return 1
	fi
}

clean()
{
	chirp_clean
	rm -f config.txt output.txt
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
