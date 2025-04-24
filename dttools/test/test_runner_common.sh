#!/bin/sh

export CC=${CC:-gcc}

# Tests sometimes chdir, so we get the full path now. `pwd` is annoying to portable.
WORK_QUEUE_WORKER=$(cd "$(dirname "$0")/../../work_queue/src/"; pwd)/work_queue_worker
TASKVINE_WORKER=$(cd "$(dirname "$0")/../../taskvine/src/worker"; pwd)/vine_worker

export GDB_WRAPPER="gdb --batch -ex run -ex bt -ex exit --args "

# Obtain a config value from the master config file.

import_config_val()
{
	name=$1
	value=$(grep "^${name}=" ../../config.mk | cut -d = -f 2)
	export ${name}=${value}
}

dispatch()
{
	case "$1" in
		check_needed)
			check_needed $@
			;;
		prepare)
			prepare $@
			;;
		run)
			run $@
			;;
		clean)
			clean $@
			;;
		*)
			echo "unknown command: $1"
			echo "use: $0 [check_needed|prepare|run|clean]"
			exit 1
			;;
	esac
	exit $?
}

# wait_for_file_creation(filename, timeout)
# Waits at most timeout seconds for filename to be created.
# Returns 0 if filename created before timeout, otherwise terminates the script.

wait_for_file_creation()
{
	filename=$1
	timeout=$2

	if [ -z $filename ]
	then
		exit 1
	fi
	
	for i in $(seq 1 $timeout)
	do
		if [ -f $filename ]
		then
			return 0
		else
			echo "waiting for $filename ($i)..."
			sleep 1
		fi
	done

	echo "$filename was not created after $timeout seconds!"
	
	exit 1
}

# wait_for_file_modification(filename, timeout)
# returns until the last modification to filename is timeout seconds (default 5) in the past.
wait_for_file_modification()
{
	filename=$1
	timeout=${2:-5}

	case `uname -s` in
		Darwin)
			args="-f %m $filename"
		;;
		*)
			args="-c %Y $filename"
		;;
	esac

	while true; do
		sleep 1
		[ ! -f $filename ] && exit 1
		mtime=`stat $args`
		now=`date +"%s"`
		delta=$(($now-$mtime))
		[  $delta -gt 3 ] && break
	done
}

run_wq_worker()
{
	local port_file=$1
	shift
	local log=$1
	shift

	local timeout=15

	if [ -z "$log" ]; then
		log=worker.log
	fi

	echo "Waiting for manager to be ready."
	if wait_for_file_creation $port_file $timeout
	then
		echo "Master is ready on port `cat $port_file` "
	else
		echo "ERROR: Master failed to respond in $timeout seconds."
		exit 1
	fi
	echo "Running worker."
	if ! "$WORK_QUEUE_WORKER" --single-shot --timeout=10s --cores ${cores:-1} --memory ${memory:-250} --disk ${disk:-250} --gpus ${gpus:-0} ${coprocess:-} --coprocess-cores ${coprocess_cores:-1} --coprocess-disk ${coprocess_disk:-250} --coprocess-memory ${coprocess_memory:-250} --coprocess-gpus ${coprocess_gpus:-0} --debug=all --debug-file="$log" $* localhost $(cat "$port_file"); then
		echo "ERROR: could not start worker"
		exit 1
	fi
	echo "Worker completed."
	return 0
}

run_taskvine_worker()
{
	local port_file=$1
	shift
	local log=$1
	shift

	local timeout=15

	if [ -z "$log" ]; then
		log=worker.log
	fi

	echo "Waiting for manager to be ready."
	if wait_for_file_creation $port_file $timeout
	then
		echo "Master is ready on port `cat $port_file` "
	else
		echo "ERROR: Master failed to respond in $timeout seconds."
		exit 1
	fi
	echo "Running worker."
	if ! "$TASKVINE_WORKER" --single-shot --timeout=10 --cores ${cores:-1} --memory ${memory:-250} --disk ${disk:-250} --gpus ${gpus:-0} --debug=all --debug-file="$log" $* localhost $(cat "$port_file"); then
		echo "ERROR: could not start worker"
		exit 1
	fi
	echo "Worker completed."
	return 0
}

require_identical_files()
{
	echo "Comparing output $1 and $2"
	if diff $1 $2
	then
		echo "$1 and $2 are the same."
		return 0
	else
		echo "ERROR: $1 and $2 differ!"
		exit 1
	fi
}

check_needed()
{
# to be implemented by individual tests that are optional.
# For an example, see chirp/test/TR_chirp_python.sh
	return 0
}

latest_vine_debug_log()
{
	base=${1:-vine-run-info}
	echo "${base}/most-recent/vine-logs/debug"
}


# For OS X
if ! echo $PATH | grep /sbin > /dev/null 2>&1; then
	export PATH=$PATH:/usr/sbin:/sbin
fi

# vim: set noexpandtab tabstop=4:
