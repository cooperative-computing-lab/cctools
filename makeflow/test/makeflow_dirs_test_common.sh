#! /bin/sh

STATUS_FILE=makeflow.status
PIDWORKER_FILE=worker.pid
PORT_FILE=makeflow.port


prepare()
{
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -rf input
	rm -rf mydir

	mkdir -p input
	echo world > input/hello

	exit 0
}

run()
{
	# send makeflow to the background, saving its exit status.
	(../src/makeflow -d all -T wq -Z $PORT_FILE  $MAKE_FILE; echo $? > $STATUS_FILE) &

	# wait at most 5 seconds for makeflow to find a port.
	wait_for_file_creation $PORT_FILE 5

	# launch the worker with the port found by makeflow.
	port=`cat $PORT_FILE` 
	../../work_queue/src/work_queue_worker -t60 localhost $port &

	# wait at most one minute for makeflow to exit.
	wait_for_file_creation $STATUS_FILE 60

	# retrieve makeflow exit status
	status=`cat $STATUS_FILE`

	[ $status != 0 ] && exit 1

	# verify that makeflow created the required files from
	# $MAKE_FILE
	for file in $PRODUCTS; do
		[ ! -f $file ] && exit 1
	done

	exit 0
}

clean()
{
	../src/makeflow -c $MAKE_FILE
	rm -f $STATUS_FILE
	rm -f $PORT_FILE

	rm -rf input
	rm -rf mydir

	[ -f $PIDWORKER_FILE ] && /bin/kill -9 `cat $PIDWORKER_FILE`

	exit 0
}


