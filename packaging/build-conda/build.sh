#!/bin/bash

if [ X$CCTOOLS_SOURCE_PROFILE = Xyes ]
then
    source ~/.bash_profile
fi

conda run --name cctools-build make
