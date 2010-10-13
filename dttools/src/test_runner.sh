#!/bin/sh

run_and_test()
{
    command=$1
    shift

    [ $verbose = 1 ] && echo -n "  $command ... "
    if $@ >> $log_file 2>&1
    then
	[ $verbose = 1 ] && echo "ok"
	success=1
    else
	[ $verbose = 1 ] && echo "fail"
	success=0
    fi
}

run_tests()
{
    for test_script in $@
    do
    	success=0
	    
	echo "--- testing $test_script" >> $log_file

	if [ $verbose = 0 ]
	then
	    echo -n "testing $test_script ... "
	else
	    echo "testing $test_script ... "
	fi

	for p in prepare run
	do
	    run_and_test $p "$test_script $p"
	    if [ $success = 0 ]
	    then
	    	break
	    fi
	done
	    
	if [ $success = 1 ]
	then
	    [ $verbose = 0 ] && echo "ok"
	    echo "=== tested  $test_script: ok" >> $log_file
	else
	    [ $verbose = 0 ] && echo "fail"
	    echo "=== tested  $test_script: fail" >> $log_file
	fi

	run_and_test clean "$test_script clean"
    done
}

show_help()
{
    echo "Use: test_runner.sh [options] test_case.sh ..."
    echo "options:"
    echo "  -l <log_file> Set log file (default is $$CCTOOLS_TEST_LOG)."
    echo "  -v            Enable verbose output."
    echo "  -h            Show this help message."

    exit 1
}

log_file=$CCTOOLS_TEST_LOG
verbose=0
success=0

if [ -z $log_file ]
then
    log_file="cctools.test.log"
fi

while getopts l:vh opt
do
    case "$opt" in
    l) log_file=$OPTARG;;
    v) verbose=1;;
    h) show_help;;
    *) show_help;;
    esac
done

export PATH=$(pwd):$PATH

shift $(($OPTIND - 1))

run_tests $@
