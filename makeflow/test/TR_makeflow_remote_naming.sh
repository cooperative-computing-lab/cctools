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

	CONVERT=`which convert`

	if [ -z "$CONVERT" ]; then
		if   [ -f /usr/bin/convert ]; then
			CONVERT=/usr/bin/convert
		elif [ -f /usr/local/bin/convert ]; then
			CONVERT=/usr/local/bin/convert
		elif [ -f /opt/local/bin/convert ]; then
			CONVERT=/opt/local/bin/convert
		fi
	fi
	cp $CONVERT ./convert

	CURL=`which curl`

	if [ -z "$CONVERT" ]; then
		if   [ -f /usr/bin/curl ]; then
			CURL=/usr/bin/curl
		elif [ -f /usr/local/bin/curl ]; then
			CURL=/usr/local/bin/curl
		elif [ -f /opt/local/bin/curl ]; then
			CURL=/opt/local/bin/curl
		fi
	fi
	cp $CURL ./curl

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
	rm -f ./convert ./curl

	exit 0
}

dispatch $@
