#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

file=hello.txt
link=link.txt
expected=expected.out
from_parrot=parrot.out

prepare()
{
	set -e 

	echo hello > $file
	ln -sf $file $link

	return 0
}

run()
{
	set -e

	stat -L --terse $file $link 2>/dev/null > $expected
	parrot -dall -- stat -L --terse $file $link > $from_parrot

	if ! diff $expected $from_parrot
	then
		return 1
	else
		return 0
	fi
}

clean()
{
	rm -rf $file $link $expected $from_parrot
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
