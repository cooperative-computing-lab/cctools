#!/bin/sh

. ../../dttools/test/test_runner_common.sh

out_dir="linker_collision_out"

prepare() {
	return 0
}

run() {
	(
		set -e
		cd linker
		printf '' > ls
		../../src/makeflow_analyze -b "$out_dir" collision.mf > tmp 2>&1
		< tmp awk '{print $2}' | sort > tmp2
		diff tmp2 expected/collision.mf
	)
	return $?
}

clean() {
  rm -rf linker/"$out_dir" linker/tmp linker/tmp2 linker/ls
  exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
