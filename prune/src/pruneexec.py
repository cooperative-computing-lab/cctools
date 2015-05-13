#!/usr/bin/env python

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


args = {'ln':[], 'targz':[], 'umbrella':False, 'function_name':None, 'function_inputs':[]}
argi = 1
while argi<len(sys.argv):
	arg = sys.argv[argi]
	if arg=='-ln':
		argi += 1
		args['ln'].append( sys.argv[argi].split('=') )
	elif arg=='-targz':
		argi += 1
		args['targz'].append( sys.argv[argi] )
	elif arg=='-umbrella':
		args['umbrella'] = True
	elif args['function_name'] is None:
		args['function_name'] = sys.argv[argi]
	else:
		args['function_inputs'].append( sys.argv[argi] )

	argi += 1

'''
parser = argparse.ArgumentParser(prog='PRUNE_EXEC', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--ln', help='local filename for remote file (local=remote)', action='append')
parser.add_argument('--targz', help='name of file to extract in working directory', action='append')
parser.add_argument('--umbrella', help='name of umbrella specification file')
parser.add_argument('function_name', default='echo', help='command to execute function')
parser.add_argument('function_inputs', nargs='*', default=['test'], help='function arg1 arg2...')
args = parser.parse_args()
#parser.print_help()
'''

#print args

debug = open('prune_debug.log','a')
newline = '\\n'

def myexec(cmd):
	global debug
	pipes = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	std_out, std_err = pipes.communicate()
	debug.write('Running command:%s%s'%(cmd,newline))
	if len(std_out)>0:
		debug.write('--stdout='+std_out+newline)
	if len(std_err)>0:
		debug.write('--stderr='+std_err+newline)
	debug.write('Command returned:%i-----------------------------%s'%(pipes.returncode,newline))




debug.write('Starting PRUNE_RUN at %s%s'%( str(time.time()), newline ))
myexec('cat PRUNE_RUN')
debug.write('Environment variables:'+newline)
myexec('env')
debug.write('Local files initially:'+newline)
myexec('find ./')

if args['ln']:
	debug.write('Linking...'+newline)
	for link in args['ln']:
		(local,remote) = link.split('=')
		myexec('ln -s %s ./%s'%(remote, local))

if args['targz']:
	debug.write('Unzipping...'+newline)
	for fname in args['targz']:
		myexec('tar -zxvf %s'%(fname))

if args['targz'] or args['ln']:
	debug.write('Local files after unzipping, linking:'+newline)
	myexec('find ./')


debug.write('Function file:'+newline)
myexec('head %s'%(args['function_name']))
if os.path.isfile(args['function_name']):
	myexec('chmod 755 %s'%(args['function_name']))


if args['umbrella']:
	arg_str = ''
	for arg in args['function_inputs']:
		arg_str += ' '+arg
	arg_str = arg_str[1:]
	cmd = "%s %s"%( args['function_name'], arg_str)

	print cmd
	debug.write('Starting operation at %s:%s'%(str(time.time()) ,newline))
	myexec(cmd)
	debug.write('Finished operation at %s.%s'%(str(time.time()) ,newline))

else:
	arg_str = ''
	for arg in args['function_inputs']:
		arg_str += ' '+arg
	arg_str = arg_str[1:]
	cmd = "%s %s"%( args['function_name'], arg_str)

	print cmd
	debug.write('Starting operation at %s:%s'%(str(time.time()) ,newline))
	myexec(cmd)
	debug.write('Finished operation at %s.%s'%(str(time.time()) ,newline))

debug.write('Environment variables after execution:'+newline)
myexec('env')

debug.write('Local files after execution:'+newline)
myexec('find ./')



"""








