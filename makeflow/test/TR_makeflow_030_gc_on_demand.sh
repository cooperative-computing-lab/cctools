#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir
test_output=`basename $0 .sh`.output

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../../src/makeflow .
	ln -sf ../syntax/collect.makeflow .
cat > ../$test_output <<EOF
7
8
6
8
EOF
	exit 0
}

run()
{
	cd $test_dir
	./makeflow -g on_demand -G 4 -j 1 collect.makeflow
	if [ $? -eq 0 ]; then
		exec diff -w ../$test_output _collect.7
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
