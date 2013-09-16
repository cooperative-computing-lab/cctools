#!/bin/sh

. ../../dttools/src/test_runner.common.sh


out_dir=makeflow_linker.shrimp.out
expected=shrimp
workflow_description=shrimpmakeflow.mf

prepare() {
	cd ../src; make
	exit $?
}

run() {
	../src/makeflow_linker -o $out_dir input/shrimp/$workflow_description

	diff -bur expected/$expected $out_dir

	exit $?
}

clean() {
	rm -rf $out_dir
}

dispatch "$@"

