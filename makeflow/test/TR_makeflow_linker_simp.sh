#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_simp_out"

prepare() {
    ln ../src/makeflow ../src/makeflow_analyze
    cd linker
    if [ -d "$out_dir" ]; then
        exit 1
    fi
    mkdir "$out_dir"
    cd ../../src/; make
    exit 0
}

run() {
    cd linker
    ../../src/makeflow_analyze -b "$out_dir" simple.mf

    if [ ! -f "$out_dir"/simple.mf ]; then
        exit 1
    fi
    exit 0
}

clean() {
    rm -rf ../src/makeflow_analyze
    cd linker
    rm -rf "$out_dir"
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
