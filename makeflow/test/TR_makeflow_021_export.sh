#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=export.dir
test_output=export.output

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../syntax/export.makeflow Makeflow
cat > ../$test_output <<EOF

1
2
$
$
EOF
	exit 0
}

run()
{
	cd $test_dir
	../../src/makeflow -dall
	if [ $? -eq 0 ]; then
		exec diff ../$test_output out.all
	else
		exit 1
	fi
}

clean()
{
	rm -fr $test_dir $test_output
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
