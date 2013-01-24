#!/bin/sh

xfile=`cat $1`
yfile=`cat $2`
dfile=`cat $3`

echo $(($xfile + $yfile + $dfile))


