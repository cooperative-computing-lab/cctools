#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

parrot_debug=parrot.debug

expected=expected.txt
output=output.txt

search() {
  parrot --timeout=5 ../src/parrot_search "$@"
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
no results
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
}

run()
{
	{
		echo ++
		search fixtures 'bar' | sort
		echo ++
		search fixtures '/bar'
		echo ++
		search fixtures 'c/bar'
		echo ++
		search fixtures '/c/bar'
		echo ++
		search fixtures 'b/bar'
		echo ++
		search fixtures '/b/bar'
		echo ++
		search fixtures 'b/foo'
		echo ++
		search fixtures '/b/foo'
		echo ++
		search fixtures '/a/b/foo'
		echo ++
		search fixtures '/*/foo'
		echo ++
		search fixtures '/*/*/foo'
		echo ++
		search fixtures '*/*r' | sort
		echo ++
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
	rm -f "$parrot_debug" "$expected" "$output" fixtures
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
