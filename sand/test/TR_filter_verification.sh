#!/bin/sh

. ../../dttools/src/test_runner.common.sh

SAND_MASTER_PID_FILE=master.pid

prepare()
{
	exit 0
}

run()
{
    cd filter_verification
    ./gen_random_sequence.pl 1000
    ./gen_random_reads.pl 1000
    ./gen_candidates.sh $SAND_MASTER_PID_FILE
    exec ./verify_candidates.pl
}

clean()
{
    cd filter_verification
    rm -f random.seq random_revcom.seq random.fa random.cfa random.cand filter.log filter.log.old worker.log port.file
    rm -rf random.cand.filter.tmp
    kill -9 `cat $SAND_MASTER_PID_FILE`
	rm -f $SAND_MASTER_PID_FILE
}

dispatch $@
