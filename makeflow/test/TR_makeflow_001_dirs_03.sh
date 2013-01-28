#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# directory as input file
MAKE_FILE=dirs/testcase.subdir.03.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@


