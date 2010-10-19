#!/bin/sh

. ../../dttools/src/test_runner.common.sh

sfxfile=date.sfx

prepare()
{
    exit 0
}

run()
{
    ${CCTOOLS_PYTHON} ./starch.py -x date $sfxfile
    exec $sfxfile
}

clean()
{
    rm -f $sfxfile
    exit 0
}

dispatch $@
