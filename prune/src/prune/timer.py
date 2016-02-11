# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, logging
import json

starts = {}
summs = {}

def start( *args ):
	global starts
	for arg in args:
		if '.' in arg:
			ar = arg.split('.')
			r = ar[0]
			starts[r] = time.time()
			for a in ar[1:]:
				r += '.'+a
				starts[r] = time.time()
		else:
			starts[arg] = time.time()

def stop( *args ):
	global starts,summs
	for arg in args:
		if '.' in arg:
			ar = arg.split('.')
			r = ar[0]
			if r in summs:
				summs[r] += time.time()-starts[r]
			else:
				summs[r] = time.time()-starts[r]
			for a in ar[1:]:
				r += '.'+a
				if r in summs:
					summs[r] += time.time()-starts[r]
				else:
					summs[r] = time.time()-starts[r]
		else:
			if arg in summs:
				summs[arg] += time.time()-starts[arg]
			else:
				summs[arg] = time.time()-starts[arg]

def add( *args ):
	global starts,summs
	duration = float(args[0])
	for arg in args[1:]:
		if '.' in arg:
			ar = arg.split('.')
			r = ar[0]
			if r in summs:
				summs[r] += duration
			else:
				summs[r] = duration
			for a in ar[1:]:
				r += '.'+a
				if r in summs:
					summs[r] += duration
				else:
					summs[r] = duration
		else:
			if arg in summs:
				summs[arg] += duration
			else:
				summs[arg] = duration


def report():
	print json.dumps(summs, sort_keys=True, indent=2, separators=(',', ': '))

def reset():
	global starts, summs

	starts = {}
	summs = {}

