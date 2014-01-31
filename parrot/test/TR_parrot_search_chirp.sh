#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ../../chirp/test/chirp-common.sh
CHIRP_SERVER=../../chirp/src/chirp_server

c="./hostport.$PPID"
parrot_debug=parrot.debug
expected=expected.txt
output=output.txt

psearch="../src/parrot_run -d all -o $parrot_debug ../src/parrot_search"

prepare()
{
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
		$psearch /chirp/$hostport/ 'bar' | sort
		echo ++
		$psearch /chirp/$hostport/ '/bar'
		echo ++
		$psearch /chirp/$hostport/ 'c/bar'
		echo ++
		$psearch /chirp/$hostport/ '/c/bar'
		echo ++
		$psearch /chirp/$hostport/ 'b/bar'
		echo ++
		$psearch /chirp/$hostport/ '/b/bar'
		echo ++
		$psearch /chirp/$hostport/ 'b/foo'
		echo ++
		$psearch /chirp/$hostport/ '/a/b/foo'
		echo ++
		$psearch /chirp/$hostport/ '/*/foo'
		echo ++
		$psearch /chirp/$hostport/ '/*/*/foo'
		echo ++
		$psearch /chirp/$hostport/ '*/*r' | sort
		echo ++
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
	chirp_clean
	rm -f "$c" "$parrot_debug" "$expected" "$output"
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
