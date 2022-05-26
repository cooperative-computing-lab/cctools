#!/bin/sh

set -ex

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

prepare()
{
	cp "$(which find)" find || return 1
	ln -sf ./find lfind || return 1
}

run()
{
	[ "$(parrot ./lfind /proc/self/exe -printf %l)" = "$(./lfind /proc/self/exe -printf %l)" ] || return 1
}

clean()
{
	rm -f find lfind
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
