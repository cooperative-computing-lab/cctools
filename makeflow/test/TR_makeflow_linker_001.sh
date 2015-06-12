#!/bin/sh

. ../../dttools/test/test_runner_common.sh

out_dir=makeflow_linker.001.out
expected=001
workflow_description=001.mf

prepare() {
	return 0
}

run() {
	(
		set -e
		cd linker
		env PATH="$PATH:$(pwd)/../../src/" ../../src/makeflow_linker --use-named -o "$out_dir" "001/$workflow_description"

		[ "$(< "$out_dir/named" awk '{print $1}')" = "Python" ] || return 1
		cp "../expected/$expected/named" "$out_dir/named"

		[ -f "$out_dir/c.sh" ] || return 1
		rm -f "$out_dir"/c.sh

		[ -d "$out_dir"/a.py/b/gzip ] || return 1
		rm -rf "$out_dir/a.py/b/gzip"

		diff -bur "../expected/$expected" "$out_dir"
	)
	return $?
}

clean() {
	rm -rf "linker/$out_dir"
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
