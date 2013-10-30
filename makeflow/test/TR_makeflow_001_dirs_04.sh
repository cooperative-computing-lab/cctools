#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# target and source files are both directories
MAKE_FILE=dirs/testcase.subdir.04.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@


# vim: set noexpandtab tabstop=4:
