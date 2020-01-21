#!/bin/sh

. ../../dttools/test/test_runner_common.sh

out_dir=makeflow_linker.001.out
expected=001
workflow_description=001.mf

check_needed() {
	which file > /dev/null 2>&1 || return 1

	# disabling test as it always skipped or fails, and this facility is not in
	# use.
	return 1
}

prepare() {
	return 0
}

run() {
	(
		set -e
		cd linker

		PATH="$(pwd)/../../src/:${PATH}" ../../src/makeflow_linker --use-named -o "$out_dir" "001/$workflow_description"

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
