#!/bin/sh

. ../../dttools/test/test_runner_common.sh

sfxfile=date.sfx

prepare()
{
	exit 0
}

run()
{
	../src/starch -c date -x date $sfxfile
	exec ./$sfxfile
}

clean()
{
	rm -f $sfxfile
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
