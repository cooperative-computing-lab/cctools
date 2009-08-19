#!/bin/bash

echo "Making sample pow's fucntion program"
gcc -g pow.c -o pow.exe

echo "Getting worker"
ln -s ../../../dttools/src/worker ./worker
echo "Getting example master"
ln -s ../src/work_queue_pow_example ./work_queue_pow_example
#echo "Starting worker"
./worker -t 5s -d all localhost 9068 &
wpid=$!
echo "Worker is process $wpid"
echo "Starting master"
cat ./pairs | ./work_queue_pow_example
echo "Checking results"
cat *.txt > example.out
diff --brief example.out example.right && echo "Files example.out and example.right are the same";
echo "Waiting for worker to exit"
wait $wpid
echo "Removing created files"
rm pow.exe work_queue_pow_example worker
rm -f *.txt
