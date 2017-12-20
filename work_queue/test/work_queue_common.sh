#!/bin/sh

. ../../dttools/test/test_runner_common.sh

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

	echo "waiting for master to get ready"
	wait_for_file_creation master.port 5

	if [ X$FOREMAN = X1 ]
	then
		echo "starting foreman"
		work_queue_worker -d all -o foreman.log -b 1 --foreman -Z foreman.port localhost `cat master.port` --timeout 20 --single-shot &
		wait_for_file_creation foreman.port 5

		port=`cat foreman.port`
	else
		port=`cat master.port`
	fi

	echo "starting worker"
	work_queue_worker -d all -o worker.log localhost $port -b 1 --timeout 20 --cores $CORES --memory-threshold 10 --memory 50 --single-shot

	echo "checking for output"
	i=0
	while [ $i -lt $TASKS ]
	do
		file=output.$i
		if [ ! -f $file ]
		then
			echo "$file is missing!"

			if [ -f foreman.log  ]
			then
				echo "foreman log:"
				cat foreman.log
			fi

			if [ -f worker.log  ]
			then
				echo "worker log:"
				cat worker.log
			fi

			return 1
		fi
		i=$((i+1))
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
