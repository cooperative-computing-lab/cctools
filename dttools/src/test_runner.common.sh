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
	echo unknown command: $1
	;;
    esac

    exit 1
}

find_free_port()
{
    netstat --numeric-ports --listening --numeric --protocol inet | grep tcp | awk '{print $4}' | grep 0.0.0.0 | awk -vFS=: '{print $2}' | sort --numeric-sort | grep --extended-regexp '9[0-9]{3}' | awk 'BEGIN {n = 9000} {if (n != $1) { print n; exit; } else { n = n+1; } }'
}
