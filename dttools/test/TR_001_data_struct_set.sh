#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    exit 0
}

run()
{
    exec data-structures/set
}

clean()
{
    exit 0
}

dispatch $@
