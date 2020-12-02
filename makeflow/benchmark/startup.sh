#!/bin/bash
# This script needs to be run from inside the cctools git repo 
# $1 will be the thing to checkout
# cctools, specifically resource monitor needs to be installed 

buildir=$(mktemp -d) 
(cd $(git rev-parse --show-toplevel)
git archive --format=tar $1 | tar -x -C $buildir)
(cd $buildir
./configure --without-system-parrot
make)
#cd benchmark
makeflow --jx master.jx -c
makeflow=$buildir/makeflow/src/makeflow makeflow --jx master.jx -j1

rm -rf $buildir 
