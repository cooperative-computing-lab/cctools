#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    exec ../src/hmac_test -v
}

clean()
{
    exit 0
}

dispatch $@
