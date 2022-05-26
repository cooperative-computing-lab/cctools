#! /bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

prepare()
{
	$0 clean
	cat > output.expected <<EOF
Mon Jan  1 00:00:00 UTC 2001
EOF
}

run()
{
	if parrot --time-stop -- date --utc 2>&1 > output.actual
	then
		require_identical_files output.actual output.expected
		return 0
	else
		return 1
	fi
}

clean()
{
	rm -f output.actual output.expected
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
