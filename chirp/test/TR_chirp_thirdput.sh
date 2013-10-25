#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c1="./hostport.1.$PPID"
c2="./hostport.2.$PPID"

prepare()
{
	chirp_start local
	echo "$hostport" > "$c1"
	chirp_start local --auth=address
	echo "$hostport" > "$c2"
	return 0
}

ITERATE=""
for ((i = 0; i < 1024; i++)); do
	ITERATE="$ITERATE $i"
done
for ((i = 2048; i < 1024*1024; i*=2)); do
	ITERATE="$ITERATE $i"
done

run()
{
	if ! [ -s "$c1" -a -s "$c2" ]; then
		return 0
	fi
	hostport1=$(cat "$c1")
	hostport2=$(cat "$c2")

	../src/chirp "$hostport2" setacl / address:127.0.0.1 rwlda

	../src/chirp "$hostport1" mkdir data
	../src/chirp "$hostport1" mkdir data/stuff
	dd if=/dev/zero bs=1M count=1 | ../src/chirp "$hostport1" put /dev/stdin /data/foo > /dev/null 2> /dev/null
	dd if=/dev/urandom bs=1M | for i in $ITERATE; do
		head -c $i | ../src/chirp "$hostport1" put /dev/stdin /data/stuff/$i > /dev/null 2> /dev/null
	done

	../src/chirp "$hostport1" thirdput /data "$hostport2" /data2

	[ "$(../src/chirp "$hostport1" md5 /data/foo | head -c32)" = "$(../src/chirp "$hostport2" md5 /data2/foo | head -c32)" ]
	for i in $ITERATE; do
		[ "$(../src/chirp "$hostport1" md5 /data/stuff/$i | head -c32)" = "$(../src/chirp "$hostport2" md5 /data2/stuff/$i | head -c32)" ]
	done

	return 0
}

clean()
{
	chirp_clean
	echo rm -f "$c1" "$c2"
	rm -f "$c1" "$c2"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
