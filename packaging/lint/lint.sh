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

# Install only necessary dependencies for linting purpose:
conda create -y -n cctools-lint -c conda-forge --strict-channel-priority make flake8 clang-format
conda activate cctools-lint

# Leave out some items that are research prototypes.
DISABLED_SYS=$(echo --without-system-{parrot,prune,umbrella,weaver})
DISABLED_LIB=$(echo --with-{readline,fuse,perl}-path\ no)

# Now build and configure in the normal way.
./configure --strict ${DISABLED_SYS} ${DISABLED_LIB} "$@"
[[ -f config.mk ]] && make clean
echo === Contents of config.mk ===
cat config.mk

if ! make lint &> cctools.lint.out
then
    echo === Contents of cctools.lint.out ===
    cat cctools.lint.out
    exit 1
else
    exit 0
fi
