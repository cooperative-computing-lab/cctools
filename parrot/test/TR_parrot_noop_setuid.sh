#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe=$PWD/parrot_fake_setuid.$PPID

prepare()
{
	gcc -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none <<EOF
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/fsuid.h>
#include <stdio.h>
#include <assert.h>
#include <grp.h>

int main() {
	uid_t uid = getuid();

	assert(setuid(uid) == 0);
	assert(setreuid(-1, uid) == 0);
	assert(setresuid(uid, -1, uid) == 0);
	assert(setresuid(-1, -1, -1) == 0);
	assert(setresuid(uid, uid, uid) == 0);

	if (uid != 0) {
		assert(setuid(0) == -1);
		assert(setreuid(0, -1) == -1);
		assert(setresuid(-1, 0, -1) == -1);
		assert(setresuid(0, 0, 0) == -1);
	}

	gid_t gid = getgid();

	assert(setgid(gid) == 0);
	assert(setregid(-1, gid) == 0);
	assert(setresgid(gid, -1, gid) == 0);
	assert(setresgid(-1, -1, -1) == 0);
	assert(setresgid(gid, gid, gid) == 0);

	if (gid != 0) {
		assert(setgid(0) == -1);
		assert(setregid(0, -1) == -1);
		assert(setresgid(-1, 0, -1) == -1);
		assert(setresgid(0, 0, 0) == -1);
	}

	return 0;
}
EOF
	return $?
}

run()
{
	parrot -- "$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
