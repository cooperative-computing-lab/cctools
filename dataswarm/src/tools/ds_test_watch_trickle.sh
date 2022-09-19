#!/bin/sh

# This sub-program is used by the main program ds_test_watch.

# This is a simple example of a program that gradually
# produces output over time.  It just logs the current
# time every 5 seconds for 50 seconds.


hostname > output

for n in 1 .. 10
do
	sleep 5
	date >> output
done

echo "done!" >> output
