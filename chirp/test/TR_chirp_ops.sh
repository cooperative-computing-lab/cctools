#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

prepare()
{
	chirp_start local --auth=hostname
	echo "$hostport" > "$c"
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	../src/chirp "$hostport" <<EOF
help
df -g
mkdir foo
mkdir bar
mv foo bar/foo
ls bar/foo
audit -r
whoami
whoareyou $hostport
ls
mkdir _test
ls
cd _test
ls
put /etc/hosts hosts.txt
cat hosts.txt
md5 hosts.txt
getacl
localpath hosts.txt
exit
EOF
	return 0
}

clean()
{
	chirp_clean
	rm -f "$c"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
