#! /usr/bin/env bash

if [[ $PY3K == 1 ]]; then
    ./configure --with-python3-path $CONDA_PREFIX --prefix $PREFIX --without-system-{allpairs,parrot,prune,sand,umbrella,wavefront}
else
    ./configure --prefix $PREFIX
fi

make && make install

