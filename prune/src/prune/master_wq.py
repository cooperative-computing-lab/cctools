# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess, shutil

import glob
import timer
from class_item import Item
from utils import *
from worker import Worker

try:
	from work_queue import *
except:
	print traceback.format_exc()
	print 'Work Queue library not available'
	sys.exit(1)


db = None
first_try = True

class Master:
	debug = True
	fails = []
	goods = {}
	bads = []


	def execute( self, call ):
		#print call
		worker = Worker( call )
		t = Task('')
		t.specify_cores(1)

		for fn in worker.all_inputs:
			# print 'in:',fn
			t.specify_file( str(worker.sandbox+fn), str(fn), WORK_QUEUE_INPUT, cache=True)

		for fn in worker.all_outputs:
			# print 'out:',fn
			t.specify_file( str(worker.sandbox+fn), str(fn), WORK_QUEUE_OUTPUT, cache=True)

		t.specify_command( 'chmod 755 task.sh; ./task.sh wq' )
		# if worker.call.step:
		# 	print worker.call.step
		t.specify_category( str(worker.call.step) )

		taskid = self.wq.submit( t )

		print 'Executing cmd (%s) in a %s sandbox: %s' % ( worker.call.body['cmd'], '+'.join(worker.env_names), worker.sandbox )

		self.workers[ taskid ] = worker
		self.worker_cnt += 1


	def start_workers( self ):
		global first_try

		timer.start('work.start.workers')
		started_worker_cnt = 0

		slots = self.wq.hungry()
		if slots==100:
			slots = 20
		#if slots>0 and slots<25:
		#	slots = 25
		batch = db.task_claim( slots )
		if batch:
			sys.stdout.write('.')
			sys.stdout.flush()
			calls = db.task_get( batch )

			for call in calls:
				self.execute( call )
				started_worker_cnt += 1
		elif len(self.workers)==0 and db.task_cnt()==0:
			time.sleep(5)
			sys.stdout.write(',')
			sys.stdout.flush()
			timer.report()
			return -1
			#sys.exit(0)
		self.total_tasks += started_worker_cnt
		timer.stop('work.start.workers')
		return started_worker_cnt


	def finish_workers( self ):
		timer.start('work.finish.workers')
		finished_worker_cnt = 0

		t = self.wq.wait(10)
		if t:
			worker = self.workers[t.id]
			print "WQ execution (%s) completed in %s: %s (return code %d)" % (worker.call.cbid, worker.sandbox, worker.call.body['cmd'], t.return_status)

			db.task_del( worker.call.cbid )
			self.worker_cnt -= 1
			del self.workers[t.id]

			print 'Output(',t.output,')'
			if t.return_status != 0:
				self.fail_cnt += 1
				# self.fails.append( worker.call )
				if self.debug( worker, t ):
					self.wq.blacklist( t.host )
					self.blacklog.write('Failed:'+t.hostname+' '+t.host+'\n')
					print 'Worker blacklisted:', t.hostname, t.host, ' Failures:',self.fail_cnt,', Successes:',self.success_cnt
					self.execute( worker.call )
					self.bads.append( t.hostname )
			else:
				self.success_cnt += 1
				if t.hostname in self.goods:
					self.goods[t.hostname] += 1
				else:
					self.goods[t.hostname] = 1
				print 'Worker succeeded:', t.hostname, t.host, ' Failures:',self.fail_cnt,', Successes:',self.success_cnt
				self.blacklog.write('Succeeded:'+t.hostname+' '+t.host+'\n')
				worker.finish()
			# print 'goods:',self.goods
			# print 'bads:',self.bads



		timer.stop('work.finish.workers')
		return finished_worker_cnt





	def __init__( self ):
		global db
		while not glob.ready:
			time.sleep(1)

		db = glob.db

		self.total_tasks = 0
		self.fail_cnt = 0
		self.success_cnt = 0


	def run( self ):
		try:
			self.wq = WorkQueue( glob.wq_port )
			print "Work Queue master started on port %i with name %s!" % (glob.wq_port, glob.wq_name)

		except:
			print "Work Queue failed to start on port %i!" % (glob.wq_port)
			sys.exit(1)


		self.wq.specify_master_mode(WORK_QUEUE_MASTER_MODE_STANDALONE)
		self.wq.specify_name(glob.wq_name)

		cctools_debug_flags_set("all")
		temp_uuid = uuid()
		cctools_debug_config_file( str(glob.wq_debug_log_pathname+'_'+temp_uuid) )
		self.wq.specify_log( str(glob.wq_log_pathname+'_'+temp_uuid) )
		self.wq.wait(1)

		self.blacklog = open(glob.base_dir+'logs/blacklist.%s.log'%temp_uuid, 'w+')

		self.workers = {}
		self.worker_cnt = 0

		while not glob.shutting_down:
			timer.start('work')

			finished_worker_cnt = self.finish_workers()

			started_worker_cnt = self.start_workers()

			timer.stop('work')

			if finished_worker_cnt == 0 and started_worker_cnt == 0:
				time.sleep(1)
			elif finished_worker_cnt == 0 and started_worker_cnt == -1:
				break

	def debug( self, worker, t ):
			call_body = worker.call.body
			full_content = True
			results = []
			sizes = []
			print 'debug:',call_body
			print 'execution attempted on: %s' % t.host
			for i, rtrn in enumerate( worker.all_outputs ):
				result = worker.sandbox+rtrn
				#result = worker.results[i]

				if os.path.isfile( result ):
					if rtrn == 'ENV_DBG.log':
						print 'DEBUG:'
						with open( result, 'r' ) as f:
							old_python = False
							for line in f:
								print line,
								if line.find('from .cjellyfish import')>=0:
									old_python = True
							if old_python:
								print "Blacklist host and re-execute task"
								return True

				else:
					d( 'exec', 'sandbox return file not found: '+rtrn )
					full_content = False

			print 'sandbox kept: '+worker.sandbox
			return True
