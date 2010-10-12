#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    cd ../src/; make
    exit 0
}

run()
{
    exec ../src/hmac_test
}

clean()
{
    exit 0
}

dispatch $@
