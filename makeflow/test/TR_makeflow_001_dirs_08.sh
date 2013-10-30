#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# dir depth larger than 2. target is a file
MAKE_FILE=dirs/testcase.subdir.08.makeflow

PRODUCTS="mydir/mysubdir/1.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@



# vim: set noexpandtab tabstop=4:
