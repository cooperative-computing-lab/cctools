# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

from Queue import Queue
import os, sys, time, traceback
import json as jsonlib
import readline
import subprocess, hashlib

import store_local
import server
#from . import cmdline_ui
import db_log
import exec_master

import glob
from utils import *


def start_prune():


	HOME = os.path.expanduser("~")
	CWD = os.getcwd()

	start_master = True
	start_worker = False
	workdir = None
	hostname = None
	port = None
	cores = None
	exec_filename = None


	argi = 1
	while argi<len(sys.argv):
		arg = sys.argv[argi]


		# --mode: specify what parts of prune to initiate (default: master+client)
		if arg in ['-m','--mode']:
			argi += 1
			mode = sys.argv[argi]
			if mode == 'master':
				start_master = True
				start_client = False
				start_worker = False
			else:
				print '%s mode not yet implemented.' % mode


		# -h <hostname>: hostname for the master (default: localhost)
		elif arg in ['-h','--host']:
			argi += 1
			hostname = sys.argv[argi]


		# -p <port>: port for the master (default: 8073)
		elif arg in ['-p','--port']:
			argi += 1
			hostname = int(sys.argv[argi])


		# -c <num_cores>: number of master (or worker) cores (default: 8)
		elif arg in ['-c','--cores']:
			argi += 1
			cores = int(sys.argv[argi])


		else:
			exec_filename = arg


		# working directory for config, log, files, etc
		# -w ./.prune: (worker mode default, use for master if exists)
		# -w ~/.prune: (default for master)
		# create if needed
		if arg in ['-w','--workdir']:
			argi += 1
			workdir = sys.argv[argi]




		elif arg in ['-v','--version']:
			#cctools_version = '5.0.0 [prune:887c027d-DIRTY]'
			#cctools_releasedate = '2015-05-26 11:56:15 -0400'
			print "prune_core version %s (released %s)"%( cctools_version, cctools_releasedate )
			sys.exit(0)
		elif arg in ['-d','--debug']:
			argi += 1
			debug_level = sys.argv[argi]
		elif arg == '--reset':
			reset_all = True
		elif arg in ['-h','--help']:
			message = '''Use: prune [options]
	prune options:
		-h,--help      Show command line options
		-v,--version      Display the version of cctools that is installed.
		-d,--debug <subsystem>      Enable debugging on worker for this subsystem. (try -d all to start)
		--reset      Truncate the database and delete all data.

		-w,--workdir <pathname>      Specify a working directory. (worker default ./.prune;  master default ~/.prune)
		-m,--mode <master|worker>      Running mode. (default: master)
		-c,--cores <num_cores>      Number of master (or worker) cores. (default: 8)
		-h,--hostname <ip|domain>      Hostname for master. (default: localhost)
		-p,--port <port_number>      Port number for master. (default: 8073)
			'''
			print message
			sys.exit(0)
		else:
			run_filename = arg
		argi += 1



	if start_worker:
		#workdir = CWD + '/.prune'
		#os.makedirs( workdir )
		print 'Worker mode not yet implemented'


	if not workdir:
		if os.path.isdir( CWD + '/.prune' ):
			workdir = CWD + '/.prune'
		elif os.path.isdir( HOME + '/.prune' ):
			workdir = HOME + '/.prune'
		else:
			workdir = HOME + '/.prune'
			os.makedirs( workdir )

	if workdir[-1] != '/':
		workdir += '/'

	cfg = None
	if os.path.isfile( workdir + 'config' ):
		with open( workdir + 'config', 'r' ) as jsonfile:
			cfg = jsonlib.load( jsonfile )
	if not cfg:
		cfg = { 'socket': { 'hostname':'127.0.0.1', 'port': 8073 },
				'repo': { 'id':uuid() },
				'cores': 8,

				'db': { 'drive':'primary', 'type':'log' },
				'cache': { 'drive':'primary' },
				'sandboxes': { 'drive':'primary', 'volatile':True },
				'drives': {
					'primary': { 'quota':1024*1024, 'location':workdir }
				}
			}
		if hostname:
			cfg['socket']['hostname'] = hostname
		else:
			hostname = cfg['socket']['hostname']
		if port:
			cfg['socket']['port'] = port
		else:
			port = cfg['socket']['port']
		if cores:
			cfg['cores'] = cores
		with open( workdir + 'config', 'w' ) as f:
			jsonlib.dump( cfg, f, sort_keys=True, indent=2, separators=(',', ': ') )

	else:
		if hostname:
			cfg['socket']['hostname'] = hostname
		else:
			hostname = cfg['socket']['hostname']
		if port:
			cfg['socket']['port'] = port
		else:
			port = cfg['socket']['port']
		if cores:
			cfg['cores'] = cores
		#Ask the user if these setting should be saved in the working directory


	master = None
	if start_master:
		print 'Starting up master at %s:%i...' % (hostname, port)
		master = Master( cfg )
		exec_master.Master( )
		master.start()
		try:
			while True:
				time.sleep(1)
		except KeyboardInterrupt:
			print "Exiting Prune"


	'''
	print "Welcome to Prune!"
	try:

		while True:
			json_str = raw_input(PROMPT).strip()
			if len(json_str)>0:
				if line[0] == '{':
					item = jsonlib.loads( line )
					if '_key' in item:
						master.messageQ.put_nowait( { 'line':line } )
					else:
						print 'Object without _key was not stored: ',line
				else:
					print '?'
	except KeyboardInterrupt:
		print "Exiting Prune"
	finally:
		master.stop()


	'' '
	if not run_filename:
		cmdline = cmdline_ui.CmdLine( master )
	elif run_filename == 'stdin':
		cmdline = cmdline_ui.CmdLine( master, sys.stdin )
	else:
		cmdline = cmdline_ui.CmdLine( master, [line.strip() for line in open(run_filename)] )
	cmdline.go()
	'''



class Master:
	cfg = {}
	db = None
	ready = False
	terminate = False
	env = None
	executer = None


	def __init__( self, new_cfg ):
		self.cfg = new_cfg
		self.master = self
		#self.start()


	def start( self ):

		if not self.ready:

			self.ready = False
			self.messageQ = Queue()

			self.stores = {}
			for drive_name in self.cfg['drives']:
				drive_info = self.cfg['drives'][drive_name]
				self.stores[drive_name] = store_local.Folder( drive_info['location'] )
				if not store_local.primary:
					store_local.primary = self.stores[drive_name]
				glob.store_local = store_local

			if 'db' in self.cfg:
				db_info = self.cfg['db']
				if db_info['type'] == 'log':
					self.db = db_log.Database( self.stores[ db_info['drive'] ] )

			if 'sandboxes' in self.cfg:
				drive_name = self.cfg['sandboxes']['drive']
				drive = self.stores[drive_name]
				self.sandbox_folder = drive.sandbox_folder( self.cfg['sandboxes']['volatile'] )
				self.sandbox_drive = drive


			self.ready = True

			if 'socket' in self.cfg:
				self.server = server.Listen( self.cfg['socket']['hostname'], self.cfg['socket']['port'] )


	def stop( self ):
		if self.ready:
			self.server.stop()
			while not self.master.messageQ.empty():
				time.sleep(0.1)


	def restart( self ):
		if self.ready:
			self.stop()
		self.start()
