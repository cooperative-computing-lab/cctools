#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

prepare()
{
	cat > default.acl <<EOF
unix:$(whoami) rwlda
address:127.0.0.1 rl
EOF
	chirp_start local --auth=address --default-acl=default.acl --inherit-default-acl
	echo "$hostport" > "$c"
	echo "$root" > "$cr"
	return 0
}

run()
{
	hostport=$(cat "$c")
	root=$(cat "$cr")

	../src/chirp -a unix "$hostport" mkdir -p /users/$(whoami)/data
	../src/chirp -a unix "$hostport" mkdir /users/$(whoami)/data/a
	mkdir "$root"/users/$(whoami)/data/b

	../src/chirp -a unix "$hostport" listacl /users/$(whoami)/data/a
	../src/chirp -a unix "$hostport" listacl /users/$(whoami)/data/b

	../src/chirp -a unix "$hostport" setacl /users/$(whoami)/data unix:$(whoami) a
	../src/chirp -a unix "$hostport" listacl /users/$(whoami)/data/b # should be nil!
	../src/chirp -a unix "$hostport" setacl /users/$(whoami)/data unix:$(whoami) rla
	mkdir "$root"/users/$(whoami)/data/b/c
	../src/chirp -a unix "$hostport" listacl /users/$(whoami)/data/b/c
	../src/chirp -a address "$hostport" ls /users/$(whoami)/data/b/c

	mkdir "$root"/foo
	rm "$root"/.__acl && return 1 # should fail, we've been using the default ACL
	../src/chirp -a address "$hostport" listacl /foo
	../src/chirp -a address "$hostport" ls /foo
	../src/chirp -a unix "$hostport" setacl / address:127.0.0.1 none
	../src/chirp -a address "$hostport" ls /foo && return 1
	mkdir "$root"/bar
	../src/chirp -a address "$hostport" ls /bar && return 1
	../src/chirp -a unix "$hostport" ls /bar

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$cr" default.acl
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
