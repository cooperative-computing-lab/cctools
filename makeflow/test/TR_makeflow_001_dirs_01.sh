#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# a single directory as a target
MAKE_FILE=dirs/testcase.subdir.01.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@


# vim: set noexpandtab tabstop=4:
