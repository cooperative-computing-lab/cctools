#!/bin/sh

. ../../dttools/src/test_runner.common.sh

sfxfile=extract_and_remove.sfx
tarfile=starch.tar.gz

prepare()
{
    cd ..; tar czvf $tarfile src; cd -; mv ../$tarfile .
    exit 0
}

run()
{
    ../src/starch -v -x tar -x rm -c 'for f in $@; do if ! tar xvf $f; then exit 1; fi ; done; rm $@' $sfxfile
    exec $sfxfile $tarfile
}

clean()
{
    rm -f $sfxfile $tarfile 
    rm -rf $(basename $tarfile .tar.gz)
    exit 0
}

dispatch $@
