#! /bin/sh

STATUS_FILE=makeflow.status
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

	run_local_worker $PORT_FILE worker.log

	# wait at most two seconds for makeflow to exit.
	wait_for_file_creation $STATUS_FILE 2

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

	[ $DELETE_MAKE_FILE ] && rm -f $MAKE_FILE

	exit 0
}



# vim: set noexpandtab tabstop=4:
