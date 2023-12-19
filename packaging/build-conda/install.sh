#! /bin/bash

if [ $OSNAME = Darwin ]
then
    source ~/.bash_profile
fi

conda run --name cctools-build make install
