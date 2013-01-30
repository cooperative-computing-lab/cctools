#!/bin/sh

dispatch() 
{
    case $1 in
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
	echo "use: $0 [prepare|run|clean]"
	;;
    esac

    exit 1
}

find_free_port()
{
    case `uname -s` in
    	Darwin)
    	netstat -n -f inet | grep tcp | awk '{print $4}' | awk -F . '{print $5}' | sort -n | awk 'BEGIN {n = 9000} {if (n != $1) { print n; exit; } else { n = n+1; } }'
    	;;
    	Linux)
    	netstat --numeric-ports --listening --numeric --protocol inet | grep tcp | awk '{print $4}' | grep 0.0.0.0 | awk -vFS=: '{print $2}' | sort --numeric-sort | awk 'BEGIN {n = 9000} {if (n != $1) { print n; exit; } else { n = n+1; } }'
    	;;
    	*)
    	netstat --numeric-ports --listening --numeric --protocol inet | grep tcp | awk '{print $4}' | grep 0.0.0.0 | awk -vFS=: '{print $2}' | sort --numeric-sort | awk 'BEGIN {n = 9000} {if (n != $1) { print n; exit; } else { n = n+1; } }'
    	;;
    esac
}

# wait_for_file_creation(filename, timeout)
# Waits at most timeout seconds (default 5) for filename to be created.
# Returns 0 if filename created before timeout, otherwise terminates the script.
wait_for_file_creation()
{
	filename=$1
	timeout=${2:-5}
	counter_seconds=0

	[ -z $filename ] && exit 1

	while [ $counter_seconds -lt $timeout ];
	do
		[ -f $filename ] && return 0
		counter_seconds=$(($counter_seconds + 1))
		sleep 1
	done

	exit 1
}

# wait_for_file_modification(filename, timeout) 
# returns until the last modification to filename is timeout seconds (default 5) in the past.
wait_for_file_modification()
{
	filename=$1
	timeout=${2:-5}

	while true; do
		sleep 1
		[ ! -f $filename ] && exit 1
		mtime=`stat -c "%Y" $filename`
		now=`date +"%s"`
		delta=$(($now - $mtime))
		[  $delta -gt 3 ] && break
	done
}

# For OS X
if ! echo $PATH | grep /sbin > /dev/null 2>&1; then
    export PATH=$PATH:/usr/sbin:/sbin
fi
