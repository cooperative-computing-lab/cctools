#!/bin/bash

OSNAME=`uname`

if [ $OSNAME = Darwin ]
then

    mkdir -p ~/miniconda3
    curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -o ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    ~/miniconda3/bin/conda init bash
    source ~/.bashrc
    conda install -n base conda-libmamba-solver
    conda create --name cctools-build --yes --quiet --channel conda-forge --strict-channel-priority --solver=libmamba python=3 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack cloudpickle packaging
else
    conda env create --name cctools-build --quiet --file environment.yml
fi

