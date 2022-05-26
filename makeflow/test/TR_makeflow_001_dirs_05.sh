#!/bin/sh

. ../../dttools/test/test_runner_common.sh

# specify everything (directories and files) in the target file
# section
MAKE_FILE=dirs/testcase.subdir.05.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch "$@"


# vim: set noexpandtab tabstop=4:
