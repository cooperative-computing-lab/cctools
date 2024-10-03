#!/bin/hh

. ../../dttools/test/test_runner_common.sh

export PATH=../src/tools:../src/worker:$PATH

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
	../src/tools/vine_benchmark -Z master.port < master.script &

	echo "waiting for master to get ready"
	wait_for_file_creation master.port 5

	port=`cat master.port`

	echo "starting worker"
	../src/worker/vine_worker -o worker.log -d all localhost $port -b 1 --timeout 20 --cores ${CORES:-1} --memory ${MEMORY:-250} --disk ${DISK:-2000} --single-shot

	echo "checking for output"
	i=0
	while [ $i -lt $TASKS ]
	do
		file=output.$i
		if [ ! -f $file ]
		then
			echo "$file is missing!"

			logfile=$(latest_vine_debug_log)
			if [ -f ${logfile}  ]
			then
				echo "master log:"
				cat ${logfile}
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
	rm -rf master.script vine-run-info vine_benchmark_info master.port worker.log output.* input.*
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
