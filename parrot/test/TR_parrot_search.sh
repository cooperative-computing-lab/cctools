#!/bin/sh

. ../../dttools/src/test_runner.common.sh

psearch="../src/parrot_run ../src/parrot_search"

expected=expected.txt
output=output.txt

prepare()
{
	cat > "$expected" <<EOF
bar
no results
bar
no results
no results
no results
foo
foo
foo
bar
EOF
}

run()
{
	{
		$psearch fixtures/a 'bar'
		$psearch fixtures/a '/bar'
		$psearch fixtures/a 'c/bar'
		$psearch fixtures/a '/c/bar'
		$psearch fixtures/a 'b/bar'
		$psearch fixtures/a '/b/bar'
		$psearch fixtures/a 'b/foo'
		$psearch fixtures/a '/b/foo'
		$psearch fixtures/a '/*/foo'
		$psearch fixtures/a '*/*r'
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
    rm -f "$expected" "$output"
}

dispatch $@
