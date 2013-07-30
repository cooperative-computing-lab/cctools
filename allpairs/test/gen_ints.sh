#! /bin/sh

filename=${1:-integer.list}
N=${2:-50}


rm -f $filename
touch $filename

I=2
while [ $I -lt $N ]; do
	echo $I > $I 
	echo $I >> $filename
	I=$((I+1))
done

