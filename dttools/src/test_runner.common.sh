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
