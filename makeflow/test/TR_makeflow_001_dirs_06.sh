#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# specify everything (directories and files) in the target file
# section, but directory at the end (order of the targets
# creation is not as specified). This should work.
MAKE_FILE=dirs/testcase.subdir.06.makeflow

PRODUCTS="mydir/1.txt mydir/2.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@



# vim: set noexpandtab tabstop=4:
