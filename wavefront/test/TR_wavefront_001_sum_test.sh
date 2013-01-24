#!/bin/sh

. ../../dttools/src/test_runner.common.sh

prepare()
{
	rm -f R.*.*
	./gen_ints_wf.sh 10
    exit 0
}

run()
{
	answer=1854882
	port=`find_free_port`

	../src/wavefront ./sum_wf.sh 9 9

	value=`cat R.9.9`
	echo $value

	exit $(($answer-$value))
}

clean()
{
	rm -f R.*.*
    exit 0
}

dispatch $@
