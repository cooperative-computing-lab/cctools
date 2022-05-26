#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
cat << EOF > jx_count.input
{"tag": "one"} {"tag": "two"}

{"tag": "three", "other": {"tag": "not-four"}}
{"tag": "four"}

EOF

	return 0
}

run()
{
	../src/jx_count_obj_test 4 jx_count.input
}

clean()
{
	rm -f jx_count.input
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
