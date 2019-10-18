#!/usr/bin/env python

# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
import traceback

from prune import glob
from prune.utils import *
from prune.class_item import Item

from prune import db_sqlite
#from prune import db_net

from prune import worker_local
#from prune import worker_wq

#import worker_wq
#import exec_wq


glob.cctools_version = 'CCTOOLS_VERSION'
glob.cctools_releasedate = 'CCTOOLS_RELEASE_DATE'


HOME = os.path.expanduser("~")+'/'
CWD = os.getcwd()+'/'

worker_type = 'local'
concurrency = 8
hostname = None
port = None

try:
	argi = 1
	while argi<len(sys.argv):
		arg = sys.argv[argi]


		# --type: specify what kind of workers to use (default: local)
		if arg in ['-t','-T','--type']:
			argi += 1
			worker_type = sys.argv[argi]

		# -c <concurrency>: number of concurrent workers (default: 8)
		elif arg in ['-c','-C','--cores']:
			argi += 1
			glob.exec_local_concurrency = int(sys.argv[argi])

		# -h <hostname>: hostname for the master (default: None (connect to local database))
		elif arg in ['-H','--host']:
			argi += 1
			hostname = sys.argv[argi]


		# -p <port>: port for the master (default: None (connect to local database))
		elif arg in ['-P','--port']:
			argi += 1
			port = int(sys.argv[argi])

		# -n <name>: work queue master name (default: prune_{uuid()})
		elif arg in ['-n','-N','--name']:
			argi += 1
			glob.wq_name = sys.argv[argi]

		# -s <stage>: task stage to execute (default: <any>)
		elif arg in ['-s','-S','--stage']:
			argi += 1
			glob.wq_stage = sys.argv[argi]


		elif arg in ['-v','--version']:
			#cctools_version = '5.0.0 [prune:887c027d-DIRTY]'
			#cctools_releasedate = '2015-05-26 11:56:15 -0400'
			print "prune_core version %s (released %s)"%( cctools_version, cctools_releasedate )
			sys.exit(0)

		elif arg in ['-d','--debug']:
			argi += 1
			debug_level = sys.argv[argi]

		elif arg in ['-h','--help']:
			message = '''Use: prune [options]
	prune options:
		-h,--help      Show command line options
		-v,--version      Display the version of cctools that is installed.
		-d,--debug <subsystem>      Enable debugging on worker for this subsystem. (try -d all to start)

		-t,--type <local|wq>      Type of workers. (default: local)
		-c,--concurrency <num_workers>      Number of concurrent workers. (default: <config file>)

		-n,--name <master_name>     Work queue master name. (default: <config file>)
			'''
#		-h,--hostname <ip|domain>      Hostname for master. (default: localhost)
#		-p,--port <port_number>      Port number for master. (default: 8073)
			print message
			sys.exit(0)

		argi += 1

	if worker_type == 'local':
		if not hostname and not port:
			glob.ready = True
			glob.db = db_sqlite.Database()
			worker = worker_local.Master()
			worker.run()
		else:
			'Network server not yet implemented'

	elif worker_type == 'wq':

		if not hostname and not port:
			glob.ready = True
			glob.db = db_sqlite.Database()
			worker = worker_wq.Master()
			worker.run()
		else:
			'Network server not yet implemented'

	else:
		print '%s mode not yet implemented.' % mode
except SystemExit:
	pass
except:
	print traceback.format_exc()
