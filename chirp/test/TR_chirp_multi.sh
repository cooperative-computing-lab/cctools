#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c1="./hostport.1.$PPID"
c2="./hostport.2.$PPID"

VOLUME="tank"
VOLUME_KEY="b63ae12fe3c8708ecae4d3cba504f5705af1440e" # `echo -n "$VOLUME" | sha1sum | awk '{print $1}'`

prepare()
{
	chirp_start local
	echo "$hostport" > "$c1"
	chirp_start local
	echo "$hostport" > "$c2"
	return 0
}

run()
{
	if ! [ -s "$c1" -a -s "$c2" ]; then
		return 0
	fi
	hostport1=$(cat "$c1")
	hostport2=$(cat "$c2")

	../src/chirp "$hostport1" mkdir "$VOLUME"
	../src/chirp "$hostport1" mkdir "$VOLUME"/root
	../src/chirp "$hostport1" put /dev/stdin "$VOLUME"/hosts <<EOF
$hostport1
$hostport2
EOF
	../src/chirp "$hostport1" put /dev/stdin "$VOLUME"/key <<EOF
$VOLUME_KEY
EOF

	# We can only test the multi interface through parrot...
	if [ -x ../../parrot/src/parrot_run ]; then
		../../parrot/src/parrot_run ls -l /multi/"$hostport1"@"$VOLUME"/
		../../parrot/src/parrot_run df -h /multi/"$hostport1"@"$VOLUME"/
		../../parrot/src/parrot_run sh -c "echo 1 > /multi/$hostport1@$VOLUME/foo"
		../../parrot/src/parrot_run sh -c "echo 2 > /multi/$hostport1@$VOLUME/bar"
	fi

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c1" "$c2"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
