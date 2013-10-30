#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_abs_out"

prepare() {
    if [ -d "$out_dir" ]; then
        exit 1
    fi
    cd ../src/; make
    exit 0
}

run() {
    cd linker
    `../../src/makeflow -b "$out_dir" absolute.mf &> tmp`
    `diff tmp expected/absolute.mf`
    exit $?
}

clean() {
    cd linker
    rm -rf "$out_dir" tmp
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
