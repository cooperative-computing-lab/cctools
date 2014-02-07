#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_deps_out"

prepare() {
    if [ -d "$out_dir" ]; then
        exit 1
    fi

    exit 0
}

run() {
    cd linker
    out=`../../src/makeflow_analyze -b "$out_dir" some_dependencies.mf | grep simple.mf`
    if [ -z "$out" ]; then
        exit 1
    fi
    exit 0
}

clean() {
    cd linker
    rm -r "$out_dir"
    exit 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
