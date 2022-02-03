#!/bin/sh

# This test evaluates the ability of parrot to load and run an executable via chirp.
# To avoid complex dependencies, this is a simple static hello-world.


set -e

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh
. ../../chirp/test/chirp-common.sh

prepare()
{
	chirp_start local
	echo "$hostport" > config.txt

	set +e
	# -static requires "libc-devel" which is missing on some platforms
	gcc -static -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o ${root}/hello.exe -x c - -x none <<EOF
#include <stdio.h>
int main (int argc, char *argv[])
{
	printf("Hello, world!\\n");
	return 0;
}
EOF
	set -e

	return 0
}

run()
{
	hostport=$(cat "config.txt")

	parrot --no-chirp-catalog --timeout=5 --work-dir="/chirp/${hostport}/" ./hello.exe > output.txt

	if [ "$(cat output.txt)" == "Hello, world!" ]
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
