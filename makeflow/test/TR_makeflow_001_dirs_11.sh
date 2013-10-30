#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# many directories. Prove on redundant transfer with debug on!
MAKE_FILE=dirs/testcase.subdir.11.makeflow

PRODUCTS="1.txt mydir.txt mydir/1.txt mydir/2.txt mydir/1/a.txt mydir/1/b.txt mydir/1/haha/lala.txt mydir/1/haha/wawa.txt"

. ./makeflow_dirs_test_common.sh

dispatch $@



# vim: set noexpandtab tabstop=4:
