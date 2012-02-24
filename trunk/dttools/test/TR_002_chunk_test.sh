#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    cd ../src/; make
    exit 0
}

run()
{
    cd ../src; exec ./chunk_test
}

clean()
{
    rm -f ../src/large_chunk.txt
    exit 0
}

dispatch $@
