#!/bin/sh

lc_a=$(wc -l "$1" | awk '{print $1}')
lc_b=$(wc -l "$2" | awk '{print $1}')

echo $(($lc_a - $lc_b))
exit 0

# vim: set noexpandtab tabstop=4:
