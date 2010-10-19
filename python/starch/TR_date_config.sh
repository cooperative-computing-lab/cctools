#!/bin/sh

. ../../dttools/src/test_runner.common.sh

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
    ${CCTOOLS_PYTHON} ./starch.py -C date.cfg $sfxfile
    exec $sfxfile
}

clean()
{
    rm -f $sfxfile $cfgfile
    exit 0
}

dispatch $@
