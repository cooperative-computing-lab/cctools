#!/bin/sh

# This sub-program is used by the main program vine_example_watch.

# This is a simple example of a program that gradually
# produces output over time.  It just logs the current
# time every second for 30 seconds.

hostname 

for n in $(seq 1 30)
do
	sleep 1 
	date
done

echo "done!"
