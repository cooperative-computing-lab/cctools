#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir

monitor_log=log_for_monitor.log

check_needed()
{
	exit 0
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
	../../src/makeflow -d all -l ${monitor_log} Makeflow &
	wait_for_file_creation ${monitor_log} 5
	../../src/makeflow_monitor -H ${monitor_log}
	exit $?
}

clean()
{
	rm -rf $test_dir
	exit 0
}


dispatch "$@"

# vim: set noexpandtab tabstop=4:
