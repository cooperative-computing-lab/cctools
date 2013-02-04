#!/bin/sh

. ../../dttools/src/test_runner.common.sh

# example of makeflow provided with the source
MAKE_FILE=../example/example.makeflow

PRODUCTS="capitol.montage.gif"

. ./makeflow_dirs_test_common.sh

dispatch $@

