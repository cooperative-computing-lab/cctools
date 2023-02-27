#!/bin/sh

# Perform a simple comparison of the words counts of each document
# which are given as the first ($1) and second ($2) command lines.

cat $1 | tr " " "\n" | sort | uniq -c | sort -rn | head -10l > a.tmp

cat $2 | tr " " "\n" | sort | uniq -c | sort -rn | head -10l > b.tmp

diff a.tmp b.tmp

exit 0

