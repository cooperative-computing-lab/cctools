#!/bin/sh

rm -f *.eps *.png

for f in *.txt; do
    ditaa $f
    ditaa_eps $f
done
