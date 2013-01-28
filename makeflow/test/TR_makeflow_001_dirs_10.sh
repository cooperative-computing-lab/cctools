#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# dir depth larger than 2. Target is the sub-directory
# 'mysubdir'.
MAKE_FILE=dirs/testcase.subdir.10.makeflow

PRODUCTS="mydir/mysubdir/1.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@

