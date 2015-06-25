# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


def script_str():
	return """#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, sys, traceback, os.path
import time, threading
import subprocess
import os.path

os.environ["HTTP_PROXY"] = "http://cache01.hep.wisc.edu:3128"

args = {'ln':[], 'gzip':[], 'gunzip':[], 'targz':[], 'umbrella':False, 'function_name':None, 'function_inputs':[]}
argi = 1
while argi<len(sys.argv):
	arg = sys.argv[argi]
	if arg=='-ln':
		argi += 1
		args['ln'].append( sys.argv[argi].split('=') )
	elif arg=='-gunzip':
		argi += 1
		args['gunzip'].append( sys.argv[argi].split('=') )
	elif arg=='-gzip':
		argi += 1
		args['gzip'].append( sys.argv[argi] )
	elif arg=='-targz':
		argi += 1
		args['targz'].append( sys.argv[argi] )
	elif arg=='-umbrella':
		args['umbrella'] = True
	elif arg=='-umbrellai':
		argi += 1
		args['umbrellai'] = sys.argv[argi]
	elif args['function_name'] is None:
		args['function_name'] = sys.argv[argi]
	else:
		args['function_inputs'].append( sys.argv[argi] )

	argi += 1

#print args

debug = open('prune_debug.log','a')
newline = '\\n'

def myexec(cmd):
	global debug
	pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	std_out, std_err = pipes.communicate()
	debug.write('%s===================%sRunning command:%s%s'%(newline,newline,cmd,newline))
	if len(std_out)>0:
		debug.write('--stdout='+std_out+newline)
	if len(std_err)>0:
		debug.write('--stderr='+std_err+newline)
	debug.write('Command returned:%i-----------------------------%s%s'%(pipes.returncode,newline,newline))

def myexec_output(cmd):
	global debug
	pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	std_out, std_err = pipes.communicate()
	return std_out




debug.write('Starting PRUNE_RUN at %s%s'%( str(time.time()), newline ))
myexec('cat PRUNE_RUN')
debug.write('Environment variables:'+newline)
myexec('env')
debug.write('Local files initially:'+newline)
myexec('ls -la')
debug.write('args:'+str(args)+newline)

if args['ln']:
	debug.write('Linking...'+newline)
	for (local,remote) in args['ln']:
		myexec('ln -s %s ./%s'%(remote, local))

if args['targz']:
	debug.write('Unzipping...'+newline)
	for fname in args['targz']:
		myexec('tar -xvf %s'%(fname))

if args['gunzip']:
	debug.write('gunzipping...'+newline)
	for (unzip,zip) in args['gunzip']:
		gunz_cmd = 'gunzip -c %s > ./%s'%(zip, unzip)
		debug.write('gunz_cmd'+newline)
		myexec(gunz_cmd)
		myexec('head ./%s'%(unzip))
		debug.write('...'+newline)
		myexec('tail ./%s'%(unzip))

if args['targz'] or args['gunzip'] or args['ln']:
	debug.write('Local files after unzipping, linking:'+newline)
	myexec('ls -la')


debug.write('Function file:'+newline)
myexec('head %s'%(args['function_name']))
if os.path.isfile(args['function_name']):
	myexec('chmod 755 %s'%(args['function_name']))







arg_str = ''
for arg in args['function_inputs']:
	arg_str += ' '+arg
arg_str = arg_str[1:]


if args['umbrella']:
	sub_cmd = "%s %s"%( args['function_name'], arg_str)
	cmd = './UMBRELLA_EXECUTABLE -s local -i %s -c ENVIRONMENT -l ./tmp/ -o ./final_output run "%s"'%(args['umbrellai'],sub_cmd)

else:
	cmd = "%s %s"%( args['function_name'], arg_str)

print cmd
debug.write('Starting operation at %s:%s'%(str(time.time()) ,newline))

exec_start = time.time()
myexec(cmd)
execution_time = time.time()-exec_start

debug.write('Finished operation at %s.%s'%(str(time.time()) ,newline))
debug.write('Execution time: %s%s'%(execution_time,newline))




if args['gzip']:
	debug.write('gzipping...'+newline)
	for filename in args['gzip']:
		myexec('gzip -c %s > ./%s.gz'%(filename, filename))


debug.write('Final files:'+newline)
myexec("du")

execution_space = myexec_output("du | tail -n 1 | awk '{print $1}'")
debug.write('Execution space: %s%s'%(execution_space,newline))



"""
