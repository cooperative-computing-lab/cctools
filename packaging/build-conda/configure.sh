#!/bin/bash

# Fix for local environment at ND: unset PYTHONPATH to ignore existing python installs.
export PYTHONPATH=

# Activate the Conda shell hooks without starting a new shell.
CONDA_BASE=$(conda info --base)
. $CONDA_BASE/etc/profile.d/conda.sh

conda activate cctools-dev

# Leave out some items that are research prototypes.
DISABLED_SYS=$(echo --without-system-{parrot,prune,umbrella,weaver})
DISABLED_LIB=$(echo --with-{readline,fuse,perl}-path\ no)

# Now configure in the normal way.
./configure --strict ${DISABLED_SYS} ${DISABLED_LIB} "$@"
[[ -f config.mk ]] && make clean
echo === Contents of config.mk ===
cat config.mk
