#!/bin/sh

# is $1 properly divisible by $2?

int1=`echo $OUTPUT | sed -n 1p $1`
int2=`echo $OUTPUT | sed -n 1p $2`
if [ $int1 -gt $int2 ]; then
	echo $(( $int1 % $int2))
else
	echo $int1
fi

exit 0

# vim: set noexpandtab tabstop=4:
