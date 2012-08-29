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

# For OS X
if ! echo $PATH | grep /sbin > /dev/null 2>&1; then
    export PATH=$PATH:/usr/sbin:/sbin
fi
