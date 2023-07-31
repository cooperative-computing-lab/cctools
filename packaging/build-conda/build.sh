#! /bin/bash

set -xe

# Save the dir from which the script was called
ORG_DIR=$(pwd)

# Find cctools src directory
CCTOOLS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

# Ensure we end up in the directory we started regardless of how the script
# ends.
function finish {
    cd ${ORG_DIR}
}
trap finish EXIT

# Fix for local environment at ND: unset PYTHONPATH to ignore existing python installs.
export PYTHONPATH=

# Activate the Conda shell hooks without starting a new shell.
CONDA_BASE=$(conda info --base)
. $CONDA_BASE/etc/profile.d/conda.sh

# Install conda developer dependencies first:
conda create -y -n cctools-dev -c conda-forge --strict-channel-priority python=3 gcc_linux-64 gxx_linux-64 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack cloudpickle
conda activate cctools-dev

# Configure and build in the normal way:
./configure --debug --strict "$@"
[[ -f config.mk ]] && make clean
echo === Contents of config.mk ===
cat config.mk

make

make install

if ! make test
then
    echo === Contents of cctools.test.fail ===
    cat cctools.test.fail
    exit 1
else
    exit 0
fi

