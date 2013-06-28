#!/bin/sh

. ../../dttools/src/test_runner.common.sh

sfxfile=example.sfx

prepare()
{
    exit 0
}

run()
{
    case `uname -s` in
        Darwin)
            cfgfile=example.osx.cfg
            ;;
        *)
            cfgfile=example.cfg
            ;;
    esac

    ../src/starch -C $cfgfile $sfxfile
    exec ./$sfxfile
}

clean()
{
    rm -f $sfxfile
    exit 0
}

dispatch $@
