#!/usr/bin/env python

import sys
import random

def usage():
    print >>sys.stderr, 'usage:', sys.argv[0], '<prefix> <nnumbers> <nfiles>'
    print >>sys.stderr, '''
Generate a set of <nfiles> files named <prefix>N where N is [0, <nfiles> - 1].
Each file contains <nnumbers> newline-delimited numbers between [0, 9999] in 
random order.

Arguments are:
  <prefix>	the file record prefix
  <nnumbers>	the amount of numbers per file
  <nfiles>	the number of files to generate
'''

def generate_data(prefix, nnumbers, nfiles):
    random.seed()

    for n in range(nfiles):
	f = open('%s%d' % (prefix, n), 'w+')
	for i in range(nnumbers):
	    f.write('%04d\n' % (random.randint(0, 9999)))
	f.close()

if len(sys.argv) != 4:
    usage()
else:
    generate_data(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
