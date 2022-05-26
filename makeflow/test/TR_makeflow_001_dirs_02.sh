#!/bin/sh

. ../../dttools/test/test_runner_common.sh

# files as target files ( should create directories on their
# paths if not exist)
MAKE_FILE=dirs/testcase.subdir.02.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch "$@"



# vim: set noexpandtab tabstop=4:
