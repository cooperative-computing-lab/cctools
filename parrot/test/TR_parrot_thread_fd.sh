#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="$0.test"

check_needed()
{
    # Do not run this test in github actions b/c ptrace operations are *very* slow there.
    if [ -n "$GITHUB_ACTION" ]
    then
        return 1
    else
        return 0
    fi
}

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lpthread -lm <<EOF
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <sys/wait.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void *setcloexec (void *arg)
{
	int fd = *(int *)arg;
	fcntl(fd, F_SETFD, FD_CLOEXEC);
	return NULL;
}

void *addpipe (void *arg)
{
	int fds[2];
	pipe(fds);
	sched_yield();
	close(fds[0]);
	close(fds[1]);
	return NULL;
}

void test (void *(*f)(void *))
{
	int fd = open("/dev/null", O_RDONLY);
	pthread_t id;
	pthread_attr_t attr[1];
	pthread_attr_init(attr);
	pthread_attr_setdetachstate(attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&id, attr, f, &fd);
	sched_yield();
	pid_t pid = fork();
	if (pid == 0) {
		execl("/bin/cat", "cat", "/dev/null", NULL);
		exit(EXIT_FAILURE);
	}
	waitpid(pid, NULL, 0);
	close(fd);
}

int main (int argc, char *argv[])
{
	int i;
	for (i = 0; i < 1000; i++) {
		test(setcloexec);
	}
	for (i = 0; i < 1000; i++) {
		test(addpipe);
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
