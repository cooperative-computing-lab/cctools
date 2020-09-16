#!/bin/bash

. ../../dttools/test/test_runner_common.sh

TESTCMD=../src/mq_store_test
TESTOUT1=mq_store.out1
TESTOUT2=mq_store.out2

prepare()
{
	return 0
}

run()
{
	$TESTCMD $TESTOUT1 $TESTOUT2 <(echo test; sleep 10; echo data) || return $?
	cmp $TESTCMD $TESTOUT1 || return $?
	cmp $TESTCMD $TESTOUT2 || return $?
	return 0
}

clean()
{
	rm -f $TESTOUT1 $TESTOUT2
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
