#! /bin/bash

echo y | conda install -y  -c conda-forge --strict-channel-priority python=3 gcc_linux-64 gxx_linux-64 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack cloudpickle packaging --experimental-solver=libmamba
