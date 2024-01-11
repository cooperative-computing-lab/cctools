#!/bin/bash

OSNAME=`uname`

if [ $OSNAME = Darwin ]
then
    # On macos, conda is not installed in the environment by default.
    # So we deploy it here, and then are force to source bash_profile in every step.
    mkdir -p ~/miniconda3
    curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -o ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    # This adds the conda configuration to bash_profile, which is executed once at login.
    ~/miniconda3/bin/conda init bash
    # Now we pull in the configuration.
    source ~/.bash_profile
    # Install mamba for faster solves.
    conda install -n base conda-libmamba-solver
    # Remove Linux specific packages from environment.
    grep -v linux environment.yml | grep -v gdb > environment.macos.yml
    # Now install using modified environment
    conda env create --name cctools-build --quiet --solver=libmamba --file environment.macos.yml
else
    # Install mamba for faster solves.
    conda install -n base conda-libmamba-solver
    conda env create --name cctools-build --quiet --solver=libmamba --file environment.yml
fi

