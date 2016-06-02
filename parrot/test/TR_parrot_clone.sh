#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="${0}.test"

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lpthread -lm <<EOF
#define _GNU_SOURCE

#include <pthread.h>

#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *fn (void *arg)
{
	(void)arg;
	sched_yield();
	fork();
	return NULL;
}

void testparallel (void)
{
	int i;
	for (i = 0; i < 1000; i++) {
		pthread_t id;
		pthread_attr_t attr[1];
		pthread_attr_init(attr);
		pthread_attr_setdetachstate(attr, PTHREAD_CREATE_DETACHED);
		pthread_create(&id, attr, fn, NULL);
	}
}

int main (int argc, char *argv[])
{
	int i;
	for (i = 0; i < 1; i++) {
		pid_t pid = fork();
		if (pid == 0) {
			testparallel();
			_exit(EXIT_SUCCESS);
		} else if (pid > 0) {
			int status;
			waitpid(pid, &status, 0);
		} else abort();
	}
	return 0;
}
EOF
	return $?
}

run()
{
	set -e
	for i in $(seq 5); do
		parrot -- ./"$exe" &
	done
	wait
	return 0
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
