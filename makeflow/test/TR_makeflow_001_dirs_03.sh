#!/bin/sh

. ../../dttools/test/test_runner_common.sh

# directory as input file
MAKE_FILE=dirs/testcase.subdir.03.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch "$@"



# vim: set noexpandtab tabstop=4:
