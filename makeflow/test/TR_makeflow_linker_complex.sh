#!/bin/sh

. ../../dttools/test/test_runner_common.sh

out_dir="linker_complex_out"
COMPLEX="/tmp/makeflow_test_complex"

prepare() {
	(
		set -e
		touch "$COMPLEX"path
		mkdir -p "$COMPLEX"/a/b/x
		printf '' > "$COMPLEX"/a/b/x/y
		chmod 777 "$COMPLEX"/a/b/x/y || true
	)
	return $?
}

run() {
	(
		set -e
		cd linker
		../../src/makeflow_analyze -b "$out_dir" complex.mf > tmp 2>&1
		< tmp awk '{print $2}' | sort > tmp2
		diff tmp2 expected/complex.mf
	)
	return $?
}

clean() {
	rm -rf linker/"$out_dir" linker/tmp linker/tmp2 "$COMPLEX" "$COMPLEX"path
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
