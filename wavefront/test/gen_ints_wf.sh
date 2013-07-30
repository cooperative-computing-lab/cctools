#!/bin/sh

filename=${1:-R}
N=${2:-10}

I=0

while [ $I -lt $N ]; do
		echo $I > ${filename}.$I.0
		echo $I > ${filename}.0.$I
		I=$((I+1))
done


