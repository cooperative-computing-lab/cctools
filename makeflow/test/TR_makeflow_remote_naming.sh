#!/bin/sh

. ../../dttools/src/test_runner.common.sh

MAKE_FILE="syntax/remote_name.makeflow"
PORT_FILE="makeflow.port"
WORKER_LOG="worker.log"
STATUS_FILE="makeflow.status"


prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE
	exit 0
}

run()
{
	# send makeflow to the background, saving its exit status.
	(../src/makeflow -d all -T wq -Z $PORT_FILE $MAKE_FILE; echo $? > $STATUS_FILE) &

	run_local_worker "$PORT_FILE" "$WORKER_LOG"

	# retrieve makeflow exit status
	status=`cat $STATUS_FILE`

	[ $status != 0 ] && exit 1

	exit 0
}

clean()
{
	../src/makeflow -T wq -c $MAKE_FILE
	rm -f $STATUS_FILE
	rm -f $PORT_FILE
	rm -f $WORKER_LOG

	exit 0
}

dispatch $@
