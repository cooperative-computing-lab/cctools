#!/bin/sh

# is $1 properly divisible by $2?

if [ $1 -gt $2 ]; then
	echo $(( $1 % $2))
else
	echo $1
fi

exit 0

# vim: set noexpandtab tabstop=4:
