#!/bin/sh

. ../../dttools/src/test_runner.common.sh

out_dir="linker_abs_out"

prepare() {
    if [ -d "$out_dir" ]; then
        exit 1
    fi
    touch /tmp/makeflow_test_absolute_path
    cd ../src/; make
    exit 0
}

run() {
    cd linker
    ../../src/makeflow -b "$out_dir" absolute.mf
    if [ ! -f "$out_dir"/makeflow_test_absolute_path ]; then
        exit 1
    fi
    exit 0
}

clean() {
    cd linker
    rm -rf /tmp/makeflow_test_absolute_path
    rm -rf "$out_dir"
    exit 0
}

dispatch $@
