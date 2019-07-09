#! /usr/bin/env bash

DISABLED=$(echo --without-system-{allpairs,parrot,prune,sand,umbrella,wavefront})

if [[ $PY3K == 1 ]]; then
    ./configure --prefix ${PREFIX} --with-python3-path ${CONDA_PREFIX} ${DISABLED}
else
    ./configure --prefix ${PREFIX} --with-python-path ${CONDA_PREFIX} ${DISABLED}
fi

make && make install

