#! /bin/sh

STATUS_FILE=makeflow.status
PORT_FILE=makeflow.port

wait_for_file_creation_mac()
{
    filename=$!
    timeout=${2:-5}
    counter_seconds=0
    
    [ -z $filename ] && exit

    while [ $counter_seconds -lt $timeout ];
    do
        ls
        # if ls -1 | grep -q "^$filename$"; then
        #     echo "File $filename exists."
        #     return 0
        # else
        #     echo "File $filename does not exist."
        # fi
        counter_seconds=$(($counter_seconds + 1))
		sleep 1
    done

    exit 1
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
    echo "HERE2"
	# wait at most 5 seconds for makeflow to find a port.
	wait_for_file_creation_mac $PORT_FILE 5
    echo "HERE3"
	run_taskvine_worker $PORT_FILE worker.log
    echo "HERE4"
	# wait for makeflow to exit.
	wait_for_file_creation $STATUS_FILE 10
    echo "HERE5"
	# retrieve makeflow exit status
	status=`cat $STATUS_FILE`
	if [ $status -ne 0 ]
	then
		exit 1
	fi
    echo "HERE6"
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
