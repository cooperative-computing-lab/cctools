#!/bin/sh

. ../../dttools/src/test_runner.common.sh

psearch="../src/parrot_run ../src/parrot_search"

expected=expected.txt
output=output.txt

prepare()
{
	cat > "$expected" <<EOF
++
bar
bar
++
noresults
++
bar
++
noresults
++
bar
++
bar
++
foo
++
foo
++
foo
++
bar
bar
++
EOF
}

run()
{
	{
		echo ++
		$psearch fixtures/a 'bar'
		echo ++
		$psearch fixtures/a '/bar'
		echo ++
		$psearch fixtures/a 'c/bar'
		echo ++
		$psearch fixtures/a '/c/bar'
		echo ++
		$psearch fixtures/a 'b/bar'
		echo ++
		$psearch fixtures/a '/b/bar'
		echo ++
		$psearch fixtures/a 'b/foo'
		echo ++
		$psearch fixtures/a '/b/foo'
		echo ++
		$psearch fixtures/a '/*/foo'
		echo ++
		$psearch fixtures/a '*/*r'
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
