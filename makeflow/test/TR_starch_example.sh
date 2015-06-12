#!/bin/sh

. ../../dttools/test/test_runner_common.sh

sfxfile=example.sfx

prepare()
{
	return 0
}

run()
{
	case "$(uname -s)" in
		Darwin)
			cfgfile=example.osx.cfg
			;;
		*)
			cfgfile=example.cfg
			;;
	esac

	../src/starch -C "$cfgfile" "$sfxfile"
	"./$sfxfile"
	return $?
}

clean()
{
	rm -f -- "$sfxfile"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
