#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
	./gen_ints_wfm.sh 10
	rm -f test.wmaster.output	
    exit 0
}

run()
{
	answer=1854882
	port=`find_free_port`

	../../dttools/src/work_queue_worker -t60 localhost $port &
	worker_pid=$!

	../src/wavefront_master -p $port ./sum_wfm.sh 10 10 test.wmaster.input test.wmaster.output

	kill $worker_pid

	value=`sed -n 's/^9 9 \([[:digit:]]*\)/\1/p' test.wmaster.output`

	exit $(($answer-$value))
}

clean()
{
	rm -f test.wmaster.input	
	rm -f test.wmaster.output	
    exit 0
}

dispatch $@
