#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh
. ../../chirp/test/chirp-common.sh
CHIRP_SERVER=../../chirp/src/chirp_server

c="./hostport.$PPID"
parrot_debug=parrot.debug
expected=expected.txt
output=output.txt

psearch() {
	parrot --no-chirp-catalog --timeout=5 ../src/parrot_search "$@"
}

prepare()
{
	mkdir -p ./fixtures/a/b/c
	echo unix:* rwl > fixtures/.__acl
	echo unix:* rwl > fixtures/a/.__acl
	echo unix:* rwl > fixtures/a/b/.__acl
	echo unix:* rwl > fixtures/a/b/c/.__acl

	touch ./fixtures/a/b/bar
	touch ./fixtures/a/b/foo
	touch ./fixtures/a/b/c/bar

	chirp_start ./fixtures
	echo "$hostport" > "$c"

	cat > "$expected" <<EOF
++
/a/b/bar
/a/b/c/bar
++
no results
++
/a/b/c/bar
++
no results
++
/a/b/bar
++
no results
++
/a/b/foo
++
/a/b/foo
++
no results
++
/a/b/foo
++
/a/b/bar
/a/b/c/bar
++
EOF
	return 0
}

run()
{
	hostport=$(cat "$c")
	{
		echo ++
		psearch /chirp/$hostport/ 'bar' | sort
		echo ++
		psearch /chirp/$hostport/ '/bar'
		echo ++
		psearch /chirp/$hostport/ 'c/bar'
		echo ++
		psearch /chirp/$hostport/ '/c/bar'
		echo ++
		psearch /chirp/$hostport/ 'b/bar'
		echo ++
		psearch /chirp/$hostport/ '/b/bar'
		echo ++
		psearch /chirp/$hostport/ 'b/foo'
		echo ++
		psearch /chirp/$hostport/ '/a/b/foo'
		echo ++
		psearch /chirp/$hostport/ '/*/foo'
		echo ++
		psearch /chirp/$hostport/ '/*/*/foo'
		echo ++
		psearch /chirp/$hostport/ '*/*r' | sort
		echo ++
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
	chirp_clean
	rm -f "$c" "$parrot_debug" "$expected" "$output" fixtures
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
