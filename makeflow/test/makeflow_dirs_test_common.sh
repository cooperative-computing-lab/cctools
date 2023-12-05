#! /bin/sh

STATUS_FILE=makeflow.status
PORT_FILE=makeflow.port

is_macos() {
    [ "$(uname -s)" = "Darwin" ]
}

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
	(../src/makeflow -d all -T vine -Z $PORT_FILE  $MAKE_FILE; echo $? > $STATUS_FILE) &

	# wait at most 5 seconds for makeflow to find a port.
    if is_macos; then
        # Execute macOS-specific code
        :
    else
        # Execute code for other operating systems
        wait_for_file_creation $PORT_FILE 5
    fi
	run_taskvine_worker $PORT_FILE worker.log

	# wait for makeflow to exit.
	wait_for_file_creation $STATUS_FILE 10

	# retrieve makeflow exit status
	status=`cat $STATUS_FILE`
	if [ $status -ne 0 ]
	then
		exit 1
	fi
	# verify that makeflow created the required files from
	# $MAKE_FILE
	for file in $PRODUCTS
	do
		if [ ! -f $file ]
		then
			exit 1
		fi
	done

	exit 0
}

clean()
{
	../src/makeflow -c $MAKE_FILE
	rm -f $STATUS_FILE
	rm -f $PORT_FILE
	rm -f worker.log

	rm -rf input
	rm -rf mydir

	[ $DELETE_MAKE_FILE ] && rm -f $MAKE_FILE

	exit 0
}



# vim: set noexpandtab tabstop=4:
