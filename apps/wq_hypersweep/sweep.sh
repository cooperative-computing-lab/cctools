#!/bin/bash

for i in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9
do
	for j in {1..10}
	do
		python3 resnet.py -b 64 -r 10 -d ${i} -e ${j} -s 195
	done
done

python3 plot.py
