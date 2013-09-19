#!/bin/sh

. ../../dttools/src/test_runner.common.sh

PATH=$(readlink -f ../src/):$(readlink -f ../../makeflow/src/):$PATH

out_dir=makeflow_linker.001.out
expected=001
workflow_description=001.mf

prepare() {
	cd ../src; make
	exit $?
}

run() {
	../src/makeflow_linker -o $out_dir input/001/$workflow_description
	explicit_dependency=$(cat $out_dir/explicit | awk '{print $1}')
	if [ "$explicit_dependency" != "Python" ]; then
		exit 1
	fi
	rm $out_dir/explicit

	diff -bur expected/$expected $out_dir
	exit $?
}

clean() {
	rm -rf $out_dir
}

dispatch "$@"

