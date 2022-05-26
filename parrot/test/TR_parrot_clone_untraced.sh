#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="${0}.test"

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lm <<EOF
#define _GNU_SOURCE

#include <fcntl.h>
#include <sched.h>
#include <syscall.h>
#include <unistd.h>

#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int fn (void *arg)
{
	(void)arg;
	sleep(1);
	return 0;
}

int main (int argc, char *argv[])
{
	int status;
	int flags = SIGCHLD|CLONE_VM|CLONE_SIGHAND|CLONE_PARENT|CLONE_THREAD|CLONE_FILES|CLONE_FS;
	char *stk = malloc(1<<20);
	pid_t pid = clone(fn, stk+(1<<20), flags|CLONE_UNTRACED, NULL); /* should cause Parrot to quit */
	fprintf(stderr, "cloned %d\\n", (int)pid);
	sleep(2);
	return 0;
}
EOF
	return $?
}

run()
{
	parrot -- ./"$exe" && return 1
	return 0
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
