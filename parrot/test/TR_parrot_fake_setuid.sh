#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe=$PWD/parrot_fake_setuid.$PPID

prepare()
{
	gcc -g -o "$exe" -x c - -x none <<EOF
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/fsuid.h>
#include <stdio.h>
#include <assert.h>

int main() {
    uid_t initial_ruid, initial_euid, initial_suid;
    gid_t initial_rgid, initial_egid, initial_sgid;
    uid_t ruid, euid, suid;
    gid_t rgid, egid, sgid;

    assert(getresuid(&initial_ruid, &initial_euid, &initial_suid) == 0);

    assert(setresuid(initial_ruid + 1, initial_euid + 2, initial_suid + 2) == 0);
    assert(setresuid(-1, -1, initial_suid + 3) == 0);
    assert(getresuid(&ruid, &euid, &suid) == 0);
    assert(ruid == initial_ruid + 1);
    assert(euid == initial_euid + 2);
    assert(suid == initial_suid + 3);

    assert(setreuid(initial_ruid + 5, initial_euid + 5) == 0);
    assert(setreuid(initial_ruid + 4, -1) == 0);
    assert(getresuid(&ruid, &euid, &suid) == 0);
    assert(ruid == initial_ruid + 4);
    assert(euid == initial_euid + 5);

    assert(setuid(initial_ruid + 6) == 0);
    assert(seteuid(initial_euid + 7) == 0);
    assert(getuid() == initial_ruid + 6);
    assert(geteuid() == initial_euid + 7);

    assert(setfsuid(0) == initial_ruid + 7);

    assert(getresgid(&initial_rgid, &initial_egid, &initial_sgid) == 0);

    assert(setresgid(initial_rgid + 1, initial_egid + 2, initial_sgid + 2) == 0);
    assert(setresgid(-1, -1, initial_sgid + 3) == 0);
    assert(getresgid(&rgid, &egid, &sgid) == 0);
    assert(rgid == initial_rgid + 1);
    assert(egid == initial_egid + 2);
    assert(sgid == initial_sgid + 3);

    assert(setregid(initial_rgid + 5, initial_egid + 5) == 0);
    assert(setregid(initial_rgid + 4, -1) == 0);
    assert(getresgid(&rgid, &egid, &sgid) == 0);
    assert(rgid == initial_rgid + 4);
    assert(egid == initial_egid + 5);

    assert(setgid(initial_rgid + 6) == 0);
    assert(setegid(initial_egid + 7) == 0);
    assert(getgid() == initial_rgid + 6);
    assert(getegid() == initial_egid + 7);

    assert(setfsgid(0) == initial_rgid + 7);

    return 0;
}
EOF
	return $?
}

run()
{
	../src/parrot_run --fake-setuid "$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
