#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# specify everything (directories and files) in the target file
# section
MAKE_FILE=dirs/testcase.subdir.05.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@

