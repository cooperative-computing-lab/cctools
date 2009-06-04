#!/usr/bin/env python

import sys

current = None
count   = 0

for pair in sys.stdin:
    key, value = pair.split('\t')

    if current is None:
	current = int(key)
	count   = int(value)
	continue

    if current == int(key):
	count  += int(value)
    else:
	print '%04d\t%d' % (current, count)
	current = int(key)
	count   = int(value)

print '%04d\t%d' % (current, count)
