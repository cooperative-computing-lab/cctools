#!/bin/bash

OSNAME=`uname`

if [ $OSNAME = Darwin ]
then

    mkdir -p ~/miniconda3
    curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -o ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    # This adds the conda configuration to bash_profile, which is executed once at login.
    ~/miniconda3/bin/conda init bash
    # Now we pull in the configuration.
    source ~/.bash_profile
    # But we also have to setup Conda in every normal shell as well:
    echo ". $HOME/miniconda3/etc/profile.d/conda.sh" >> $HOME/.bashrc
    conda install -n base conda-libmamba-solver
    # Remove Linux specific packages from environment.
    grep -v linux environment.yml > environment.macos.yml
    conda env create --name cctools-build --quiet --file environment.macos.yml
else
    conda env create --name cctools-build --quiet --file environment.yml
fi

