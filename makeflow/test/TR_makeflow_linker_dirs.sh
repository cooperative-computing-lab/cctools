#!/bin/sh

. ../../dttools/test/test_runner_common.sh

out_dir="linker_dirs_out"

prepare() {
	return 0
}

run() {
	(
		set -e
		cd linker
		../../src/makeflow_analyze -b "$out_dir" directories.mf > tmp 2>&1
		diff tmp expected/directories.mf
	)
	return $?
}

clean() {
	rm -rf linker/"$out_dir" linker/tmp
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
