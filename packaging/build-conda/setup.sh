#! /bin/bash

conda create --name cctools-build --yes --quiet --channel conda-forge --strict-channel-priority --experimental-solver=libmamba python=3 gcc_linux-64 gxx_linux-64 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack cloudpickle packaging
