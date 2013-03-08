#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_INPUT=R

prepare()
{
	rm -f $TEST_INPUT
	rm -f R.*.*
	./gen_ints_wf.sh $TEST_INPUT 10
    exit 0
}

run()
{
	answer=1854882

	../src/wavefront ./sum_wf.sh 9 9

	value=`cat R.9.9 | sed -n 's/[[:blank:]]*\([[:digit:]]*\).*/\1/p'`

	if [ -z $value ];
	then
		exit 1
	else
		exit $(($answer-$value))
	fi
}

clean()
{
	rm -f $TEST_INPUT
	rm -f R.*.*
    exit 0
}

dispatch $@
