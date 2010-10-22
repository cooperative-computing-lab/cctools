#!/bin/sh

. ../../dttools/src/test_runner.common.sh

sfxfile=example.sfx
cfgfile=example.cfg

prepare()
{
    exit 0
}

run()
{
    ${CCTOOLS_PYTHON} ./starch.py -C $cfgfile $sfxfile
    exec $sfxfile
}

clean()
{
    rm -f $sfxfile
    exit 0
}

dispatch $@
