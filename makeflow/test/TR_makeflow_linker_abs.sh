#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_abs_out"

prepare() {
    if [ -d "$out_dir" ]; then
        exit 1
    fi
    ln ../src/makeflow ../src/makeflow_util
    cd ../src/; make
    exit 0
}

run() {
    cd linker
    `../../src/makeflow_util -b "$out_dir" absolute.mf &> tmp`
    `diff tmp expected/absolute.mf`
    exit $?
}

clean() {
    rm -rf ../src/makeflow_util
    cd linker
    rm -rf "$out_dir" tmp
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
