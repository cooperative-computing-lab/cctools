#! /bin/bash
mkdir -p ~/miniconda3
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh -o ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
~/miniconda3/bin/conda init zsh

conda create --name cctools-build --yes --quiet --channel conda-forge --strict-channel-priority --experimental-solver=libmamba python=3 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack cloudpickle packaging
