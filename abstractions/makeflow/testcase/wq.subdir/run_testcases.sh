#!/bin/bash

numOfTests=10

rm worker
cp ~ccl/work/lyu2/cctools/dttools/src/worker .

rm makeflow
cp ~ccl/work/lyu2/cctools/abstractions/makeflow/src/makeflow .

./worker localhost 9091 &
workerpid=$!
echo -e "\n"

for((i=1;i<=numOfTests;i+=1)); do
	./makeflow -c testcase.subdir.${i}.makeflow &> /dev/null
done

for((i=1;i<=numOfTests;i+=1)); do
	echo -e "**********  TESTCASE ${i}  **********\n"
	./makeflow -T wq -p 9091 testcase.subdir.${i}.makeflow
	rv[${i}]=$?
	./makeflow -c testcase.subdir.${i}.makeflow &> /dev/null
	echo -e "\n"
done

kill -9 $workerpid

echo -e "\n**********  SUMMARY  **********\n"
for((i=1;i<=numOfTests;i+=1)); do
	echo testcase.subdir.${i}.makeflow exited with status ${rv[${i}]}
done
