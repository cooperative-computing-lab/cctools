# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, time, traceback, logging
import json

import glob

starts = {}
summs = {}
cnts = {}

timer_on = False

def start( *args ):
	pass
	# if timer_on:
	# 	global starts
	# 	for arg in args:
	# 		starts[arg] = time.time()


def stop( *args ):
	return 0.0
	# if timer_on:
	# 	global starts,summs
	# 	for arg in args:
	# 		diff = time.time()-starts[arg]
	# 		if arg in summs:
	# 			summs[arg] += diff
	# 			cnts[arg] += 1
	# 		else:
	# 			summs[arg] = diff
	# 			cnts[arg] = 1
	# 	return diff
	# else:
	# 	return 0.0

def add( *args ):
	pass
	# if timer_on:
	# 	global starts,summs
	# 	duration = float(args[0])
	# 	for arg in args[1:]:
	# 		if arg in summs:
	# 			summs[arg] += duration
	# 			cnts[arg] += 1
	# 		else:
	# 			summs[arg] = duration
	# 			cnts[arg] = 1


def report():
	pass
	# if timer_on:
	# 	if glob.workflow_id and glob.workflow_step:
	# 		with open(glob.timer_log, 'a') as f:
	# 			f.write( '%s\n' % time.time() )
	# 			f.write( glob.workflow_id + "\n" )
	# 			f.write( glob.workflow_step + "\n" )
	# 			f.write( json.dumps(summs, sort_keys=True, indent=2, separators=(',', ': ')) + "\n\n" )
	# 			f.write( json.dumps(cnts, sort_keys=True, indent=2, separators=(',', ': ')) + "\n\n" )
	# 	print
	# 	print json.dumps(summs, sort_keys=True, indent=2, separators=(',', ': '))
	# 	print json.dumps(cnts, sort_keys=True, indent=2, separators=(',', ': '))
	# 	print

def reset():
	pass
	# if timer_on:
	# 	global starts, summs, cnts

	# 	starts = {}
	# 	summs = {}
	# 	cnts = {}
