#!/bin/sh

. ../../dttools/src/test_runner.common.sh

psearch="../src/parrot_run ../src/parrot_search"

expected=expected.txt
output=output.txt

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
		$psearch fixtures 'bar' | sort
		echo ++
		$psearch fixtures '/bar'
		echo ++
		$psearch fixtures 'c/bar'
		echo ++
		$psearch fixtures '/c/bar'
		echo ++
		$psearch fixtures 'b/bar'
		echo ++
		$psearch fixtures '/b/bar'
		echo ++
		$psearch fixtures 'b/foo'
		echo ++
		$psearch fixtures '/b/foo'
		echo ++
		$psearch fixtures '/a/b/foo'
		echo ++
		$psearch fixtures '/*/foo'
		echo ++
		$psearch fixtures '/*/*/foo'
		echo ++
		$psearch fixtures '*/*r' | sort
		echo ++
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
    rm -f "$expected" "$output"
}

dispatch $@
