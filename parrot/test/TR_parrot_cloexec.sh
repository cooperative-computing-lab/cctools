#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="$0.test"

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lpthread -lm <<EOF
#include <fcntl.h>
#include <unistd.h>

#include <sys/ioctl.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef FIOCLEX
#	define FIOCLEX 0x5451
#endif

int main (int argc, char *argv[])
{
	int fd = open("/dev/null", O_RDONLY);
	if (ioctl(fd, FIOCLEX, 0) == -1) {
		perror("ioctl");
		exit(EXIT_FAILURE);
	}
	return 0;
}
EOF
	return $?
}

run()
{
	parrot -- ./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
