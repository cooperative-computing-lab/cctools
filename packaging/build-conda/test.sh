#!/bin/bash

if [ X$CCTOOLS_SOURCE_PROFILE = Xyes ]
then
    source ~/.bash_profile
fi

if ! conda run --name cctools-build make test
then
    echo === Contents of cctools.test.fail ===
    cat cctools.test.fail
    exit 1
else
    exit 0
fi

