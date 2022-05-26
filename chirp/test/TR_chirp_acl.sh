#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh

. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

prepare()
{
	cat > default.acl <<EOF
unix:$(whoami) rwlda
unix:root rwlda
address:127.0.0.1 rl
EOF
	DEFAULT_ACL=default.acl
	chirp_start local --auth=address
	echo "$hostport" > "$c"
	echo "$root" > "$cr"
	return 0
}

run()
{
	hostport=$(cat "$c")
	root=$(cat "$cr")

	chirp -a unix "$hostport" mkdir -p /users/$(whoami)/data
	chirp -a unix "$hostport" mkdir /users/$(whoami)/data/a
	mkdir "$root"/users/$(whoami)/data/b

	chirp -a unix "$hostport" listacl /users/$(whoami)/data/a
	chirp -a unix "$hostport" listacl /users/$(whoami)/data/b

	chirp -a unix "$hostport" setacl /users/$(whoami)/data unix:$(whoami) a
	chirp -a unix "$hostport" listacl /users/$(whoami)/data/b # should be nil!
	chirp -a unix "$hostport" setacl /users/$(whoami)/data unix:$(whoami) rla
	mkdir "$root"/users/$(whoami)/data/b/c
	chirp -a unix "$hostport" listacl /users/$(whoami)/data/b/c
	chirp -a address "$hostport" ls /users/$(whoami)/data/b/c

	mkdir "$root"/foo
	rm "$root"/.__acl && return 1 # should fail, we've been using the default ACL
	chirp -a address "$hostport" listacl /foo
	chirp -a address "$hostport" ls /foo
	chirp -a unix "$hostport" setacl / address:127.0.0.1 none
	chirp -a address "$hostport" ls /foo && return 1
	mkdir "$root"/bar
	chirp -a address "$hostport" ls /bar && return 1
	chirp -a unix "$hostport" ls /bar

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
