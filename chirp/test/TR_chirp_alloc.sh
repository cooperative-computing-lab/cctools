#!/bin/sh

set -ex

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c1="./hostport.1.$PPID"

prepare()
{
	chirp_start local --root-quota=65536
	echo "$hostport" > "$c1"
	return 0
}

run()
{
	if ! [ -s "$c1" ]; then
		return 0
	fi
	hostport1=$(cat "$c1")

	../src/chirp "$hostport1" mkdir data
	dd if=/dev/zero bs=64k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo || return 1
	dd if=/dev/zero bs=64k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo || return 1
	dd if=/dev/zero bs=65k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo && return 1
	dd if=/dev/zero bs=64k count=2 | ../src/chirp "$hostport1" put /dev/stdin /data/foo && return 1
	dd if=/dev/zero bs=64k count=2 | ../src/chirp "$hostport1" put /dev/stdin /data/foo && return 1
	../src/chirp "$hostport1" lsalloc /data
	../src/chirp "$hostport1" rm /data/foo

	../src/chirp "$hostport1" mkalloc /data/mydata 4096
	../src/chirp "$hostport1" lsalloc /data/mydata
	dd if=/dev/zero bs=64k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo && return 1
	dd if=/dev/zero bs=61440 count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo || return 1
	dd if=/dev/zero bs=61441 count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo && return 1
	dd if=/dev/zero bs=4k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/mydata/foo1 || return 1
	dd if=/dev/zero bs=4k count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/mydata/foo2 && return 1

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c1"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
