#!/bin/bash

echo "Making sample align program"
gcc -g alignment.c compress.c main.c -o align

echo "Getting worker"
ln -s ../../../dttools/src/worker ./worker
echo "Getting assembly_master"
ln -s ../src/assembly_master ./assembly_master
echo "Starting worker"
./worker -t 5s -d all localhost 9090 &
wpid=$!
echo "Worker is process $wpid"
echo "Starting assembly_master"
./assembly_master -n 3 -p 9090 align test6.cand test6.cfa test6.out
echo "Checking results"
diff --brief test6.out test6.right && echo "Files test6.out and test6.right are the same";
echo "Waiting for worker to exit"
wait $wpid
echo "Removing created files"
rm align assembly_master worker
rm -i test6.out
