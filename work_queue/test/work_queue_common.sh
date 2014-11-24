#!/bin/sh

. ../../dttools/src/test_runner.common.sh

export PATH=../src:$PATH

prepare()
{
	echo "nothing to do"
}

run()
{
	cat > master.script << EOF
submit 1 0 1 $TASKS
wait
quit
EOF

	echo "starting master"
	work_queue_test -d all -o master.log -Z master.port < master.script &
	masterpid=$!

	echo "waiting for master to get ready"
	wait_for_file_creation master.port 5

	if [ X$FOREMAN = X1 ]
	then
		echo "starting foreman"
		work_queue_worker -d all -o foreman.log --foreman -Z foreman.port localhost `cat master.port` --timeout 10 --single-shot &
		foremanpid=$!
		wait_for_file_creation foreman.port 5

		echo "starting worker"
		work_queue_worker -d all -o worker.log localhost `cat foreman.port` --timeout 10 --cores $CORES --single-shot

		echo "killing foreman"
		kill -9 $foremanpid
	else
		echo "starting worker"
		work_queue_worker -d all -o worker.log localhost `cat master.port` --timeout 10 --cores $CORES --single-shot
	fi

	echo "killing master"
	kill -9 $masterpid

	echo "checking for output"
	for (( i=0; i<count; i++ ))
	do
		file=output.$i
		if [ ! -f $file ]
		then
			echo "$file is missing!"
			return 1
		fi
	done

	echo "all output present"
	return 0
}

clean()
{
	rm -f master.script master.log master.port foreman.log foreman.port worker.log output.* input.*
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
