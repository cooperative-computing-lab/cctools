#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../syntax/category_variable_scope.makeflow Makeflow
	exit 0
}

run()
{
	cd $test_dir
	../../src/makeflow -d all;
	if [ $? -eq 0 ]; then
		exec diff out_rule_2a out_rule_2b
	else
		exit 1
	fi
}

clean()
{
	rm -rf $test_dir
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
