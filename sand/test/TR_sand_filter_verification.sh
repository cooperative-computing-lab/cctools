#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
	exit 0
}

run()
{
    cd filter_verification
    ./gen_random_sequence.pl 1000
    ./gen_random_reads.pl 1000
    ./gen_candidates.sh
    exec ./verify_candidates.pl
}

clean()
{
    cd filter_verification
    exec ./cleanup.sh
}

dispatch $@
