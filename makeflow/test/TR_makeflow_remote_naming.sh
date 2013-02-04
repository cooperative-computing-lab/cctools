#!/bin/sh

. ../../dttools/src/test_runner.common.sh

#  remote name translation
MAKE_FILE=syntax/remote_name.makeflow

PRODUCTS="capitol.montage.gif"

. ./makeflow_dirs_test_common.sh

dispatch $@

