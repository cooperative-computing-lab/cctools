#!/bin/sh

N=${1:-10}

filename="R"

I=0

while [[ $I -lt $N ]]; do
		echo $I > ${filename}.$I.0
		echo $I > ${filename}.0.$I
		I=$(($I + 1))
done


