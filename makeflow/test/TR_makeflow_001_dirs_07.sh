#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# second rule should not have to create 'mydir' in its command
MAKE_FILE=dirs/testcase.subdir.07.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt mydir/3.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@



# vim: set noexpandtab tabstop=4:
