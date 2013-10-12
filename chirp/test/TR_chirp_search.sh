#!/bin/sh

set -e

. ../../dttools/src/test_runner.common.sh
. ./chirp-common.sh

c="./hostport.$PPID"

expected=expected.txt
output=output.txt

prepare()
{
	cat > "$expected" <<EOF
++
/a/bar
/b/bar
++
1
++
/a/bar
++
/foo/a/bar
++
++
/foo/a/bar
++
/foo/a/bar
/foo/b/bar
++
/a/bar
/b/bar
++
/a/bar
/b/bar
++
/foo/a/bar
/foo/b/bar
++
/foo/a
/foo/a/bar
/foo/b
/foo/b/bar
++
/foo/b/bar
++
EOF

	chirp_start local
	echo "$hostport" > "$c"
	return 0
}

run()
{
	if ! [ -s "$c" ]; then
		return 0
	fi
	hostport=$(cat "$c")

	../src/chirp "$hostport" mkdir foo
	../src/chirp "$hostport" mkdir foo/a
	../src/chirp "$hostport" mkdir foo/a/bar
	../src/chirp "$hostport" mkdir foo/b
	../src/chirp "$hostport" mkdir foo/b/bar

	{
		echo ++
		../src/chirp "$hostport" search     'foo' 'bar' | sort
		echo ++
		../src/chirp "$hostport" search -s  'foo' 'bar' | wc -l
		echo ++
		../src/chirp "$hostport" search     'foo' 'a/bar' | sort
		echo ++
		../src/chirp "$hostport" search -i  'foo' 'a/bar' | sort
		echo ++
		../src/chirp "$hostport" search     'foo' '/foo/a/bar' | sort
		echo ++
		../src/chirp "$hostport" search     '/' '/foo/a/bar' | sort
		echo ++
		../src/chirp "$hostport" search     '/' '/foo/*/bar' | sort
		echo ++
		../src/chirp "$hostport" search     '/foo' '/*/bar' | sort
		echo ++
		../src/chirp "$hostport" search     '/foo' '*/bar' | sort
		echo ++
		../src/chirp "$hostport" search -i  '/foo' '*/bar' | sort
		echo ++
		../src/chirp "$hostport" search -i  '/foo' '*' | sort
		echo ++
		../src/chirp "$hostport" search -i  '/' '/foo/b/bar' | sort
		echo ++
	} | tr -d ' ' > "$output"
	diff "$expected" "$output"

	return 0
}

clean()
{
	chirp_clean
	rm -f "$c" "$output" "$expected"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
