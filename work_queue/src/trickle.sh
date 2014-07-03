#!/bin/sh

hostname > output

for n in 1 .. 10
do
	sleep 5
	date >> output
done

echo "done!" >> output
