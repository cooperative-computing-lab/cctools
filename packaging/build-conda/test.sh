#!/bin/bash

set -xe

# Fix for local environment at ND: unset PYTHONPATH to ignore existing python installs.
export PYTHONPATH=

# Activate the Conda shell hooks without starting a new shell.
CONDA_BASE=$(conda info --base)
. $CONDA_BASE/etc/profile.d/conda.sh

conda activate cctools-dev

if ! make test
then
    echo === Contents of cctools.test.fail ===
    cat cctools.test.fail
    exit 1
else
    exit 0
fi

