#!/bin/sh

. ../../dttools/test/test_runner_common.sh

sfxfile=date.sfx
cfgfile=date.cfg

prepare()
{
	exec cat > $cfgfile << EOF
[starch]
executables = date
EOF
}

run()
{
	../src/starch -c date -C $cfgfile $sfxfile
	exec ./$sfxfile
}

clean()
{
	rm -f $sfxfile $cfgfile
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
