#!/bin/sh

xfile=`cat $3`
yfile=`cat $4`
dfile=`cat $5`

echo $(($xfile + $yfile + $dfile))



# vim: set noexpandtab tabstop=4:
