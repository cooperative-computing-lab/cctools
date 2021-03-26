#!/bin/sh

. ../../dttools/test/test_runner_common.sh

export PATH=../src:$PATH
export VALGRIND="valgrind --error-exitcode=1 --leak-check=full"

test_dir=`basename $0 .sh`.dir

check_needed()
{
	if ! ${VALGRIND} --version > /dev/null 2>&1
	then
		exit 1
	fi
}


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
	exec ${VALGRIND} --log-file=manager.valgrind -- ../../src/makeflow -d all
}

clean()
{
	rm -rf $test_dir
	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
