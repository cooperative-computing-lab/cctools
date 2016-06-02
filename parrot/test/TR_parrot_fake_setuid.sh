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
	uid_t initial_ruid, initial_euid, initial_suid;
	gid_t initial_rgid, initial_egid, initial_sgid;
	uid_t ruid, euid, suid;
	gid_t rgid, egid, sgid;
	int ngroups;
	gid_t groups[128];

	assert(getgroups(128, groups) != -1);
	groups[0] = 9;
	assert(setgroups(1, groups) == 0);
	groups[0] = 0;
	assert(getgroups(128, groups) == 1);
	assert(groups[0] == 9);

	assert(getresuid(&initial_ruid, &initial_euid, &initial_suid) == 0);

	assert(setresuid(initial_ruid + 1, -1, initial_suid + 2) == 0);
	assert(getresuid(&ruid, &euid, &suid) == 0);
	assert(ruid == initial_ruid + 1);
	assert(euid == initial_euid);
	assert(suid == initial_suid + 2);

	assert(setreuid(initial_ruid, initial_euid + 5) == 0);
	assert(getresuid(&ruid, &euid, &suid) == 0);
	assert(ruid == initial_ruid);
	assert(euid == initial_euid + 5);
	assert(suid == initial_suid + 5);

	assert(setuid(initial_ruid + 6) == 0);
	assert(seteuid(initial_euid + 7) == -1);
	assert(getuid() == initial_ruid + 6);
	assert(geteuid() == initial_euid + 6);

	assert(setfsuid(0) == initial_euid + 6);


	assert(getresgid(&initial_rgid, &initial_egid, &initial_sgid) == 0);

	assert(setresgid(initial_rgid + 1, -1, initial_sgid + 2) == 0);
	assert(getresgid(&rgid, &egid, &sgid) == 0);
	assert(rgid == initial_rgid + 1);
	assert(egid == initial_egid);
	assert(sgid == initial_sgid + 2);

	assert(setregid(initial_rgid, initial_egid + 5) == 0);
	assert(getresgid(&rgid, &egid, &sgid) == 0);
	assert(rgid == initial_rgid);
	assert(egid == initial_egid + 5);
	assert(sgid == initial_sgid + 5);

	assert(setgid(initial_rgid + 6) == 0);
	assert(setegid(initial_egid + 7) == -1);
	assert(getgid() == initial_rgid + 6);
	assert(getegid() == initial_egid + 6);

	assert(setfsgid(0) == initial_egid + 6);

	return 0;
}
EOF
	return $?
}

run()
{
	parrot --fake-setuid -U 0 -G 0 -- "$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
