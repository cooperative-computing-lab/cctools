#!/bin/sh

. ../../dttools/src/test_runner.common.sh

name=`basename "$0" .sh`
exe="$name.test"

prepare()
{
	gcc -g -o "$exe" -x c - -x none -lm <<EOF
#include <unistd.h>

#ifdef __linux__
#include <sys/personality.h>
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main (int argc, char *argv[])
{
	int persona;
#ifdef __linux__
	persona = personality(0xffffffff);
	assert(persona == PER_LINUX || persona == PER_LINUX_32BIT);
#endif
	return 0;
}
EOF
	return $?
}

run()
{
	../src/parrot_run ./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"
