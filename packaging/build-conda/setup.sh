#!/bin/bash

OSNAME=`uname`

if [ $OSNAME = Darwin ]
then
    # On macos, conda is not installed in the environment by default.
    # So we deploy it here, and then are force to source bash_profile in every step.
    mkdir -p ~/miniforge3
    curl -L "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh" -o ~/miniforge3/miniforge.sh
    bash ~/miniforge3/miniforge.sh -b -u -p ~/miniforge3
    # This adds the conda configuration to bash_profile, which is executed once at login.
    ~/miniforge3/bin/conda init bash
    # Now we pull in the configuration.
    source ~/.bash_profile
    # Install mamba for faster solves.
    conda install -n base conda-libmamba-solver
    # Remove Linux specific packages from environment.
    grep -v linux environment.yml | grep -v -E '(gdb|fuse)' > environment.macos.yml
    # Now install using modified environment
    conda env create --name cctools-build --quiet --solver=libmamba --file environment.macos.yml
else
    # Install mamba for faster solves.
    conda install -n base conda-libmamba-solver
    conda env create --name cctools-build --quiet --solver=libmamba --file environment.yml
fi

