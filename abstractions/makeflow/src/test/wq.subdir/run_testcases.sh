#!/bin/bash

echo Start Work Queue Directory as Target/Source Tests ...
numOfTests=10

# set up test environment
rm -f worker 
cp ~ccl/work/lyu2/cctools/dttools/src/worker . 

rm -f makeflow 
cp ~ccl/work/lyu2/cctools/abstractions/makeflow/src/makeflow . 

rm -rf input 
rm -rf mydir 

mkdir -p input 
echo world > input/hello 

# start a worker
./worker localhost 9091 &
workerpid=$!

# makeflow cleanup
for((i=1;i<=numOfTests;i+=1)); do
	./makeflow -c testcase.subdir.${i}.makeflow &> /dev/null
done

echo
# run testcases
for((i=1;i<=numOfTests;i+=1)); do
	echo -e "Running TEST CASE ${i} ..."
	head -n 1 testcase.subdir.${i}.makeflow
	./makeflow -T wq -p 9091 testcase.subdir.${i}.makeflow &> /dev/null
	rv[${i}]=$?
	./makeflow -c testcase.subdir.${i}.makeflow &> /dev/null
	echo -e "TEST CASE ${i} is done.\n"
done

kill -9 $workerpid &> /dev/null

# print test summary
echo -e "**********  SUMMARY  **********"
for((i=1;i<=numOfTests;i+=1)); do
	if [ ${rv[${i}]} -eq 0 ] ; then
		echo TEST CASE ${i} passed successfully.
	else 
		echo TEST CASE ${i} failed. 
	fi

done

# clean up
rm -f worker
rm -f makeflow
rm -rf input
rm -rf mydir
