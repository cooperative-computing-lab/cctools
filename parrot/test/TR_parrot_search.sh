#!/bin/sh

. ../../dttools/src/test_runner.common.sh

parrot_debug=parrot.debug

expected=expected.txt
output=output.txt

search() {
  ../src/parrot_run --debug=all --debug-file="$parrot_debug" --debug-rotate-max=0 ../src/parrot_search "$@"
}

prepare()
{
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
    rm -f "$parrot_debug" "$expected" "$output"
}

dispatch $@

# vim: set noexpandtab tabstop=4:
