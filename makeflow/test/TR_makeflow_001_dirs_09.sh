#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# a single directory as a target
MAKE_FILE=dirs/testcase.subdir.09.makeflow

PRODUCTS="mydir/mysubdir/1.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@


# vim: set noexpandtab tabstop=4:
