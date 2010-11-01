#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
    cd ../src/; make
    cd ../test/filter_verification
    ./gen_random_sequence.pl 100000
    ./gen_random_reads.pl 100000
    exec ./gen_candidates.sh
}

run()
{
    cd filter_verification
    exec ./verify_candidates.pl
}

clean()
{
    cd filter_verification
    exec ./cleanup.sh
}

dispatch $@
