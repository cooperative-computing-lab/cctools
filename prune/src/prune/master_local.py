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

first_try = True

class Master:
	debug = True
	my_env = os.environ



	def execute( self, call ):
		#print call
		worker = Worker( call )

		os.chmod( worker.sandbox + 'task.sh', 0755);
		worker.process = subprocess.Popen( "./task.sh", cwd=worker.sandbox, env=self.my_env,
			stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

		print '\nExecuting cmd (%s) in a %s sandbox: %s' % ( worker.call.body['cmd'], '+'.join(worker.env_names), worker.sandbox )

		self.workers.append( worker )



	def start_workers( self ):
		global first_try

		timer.start('work.start.workers')
		started_worker_cnt = 0
		slots = glob.exec_local_concurrency - len(self.workers)
		batch = db.task_claim( slots )
		sys.stdout.write('.')
		sys.stdout.flush()
		if batch:
			calls = db.task_get( batch )
			for call in calls:
				self.execute( call )
				started_worker_cnt += 1
		elif len(self.workers)==0 and db.task_remain( glob.workflow_id )==0 and self.total_tasks>0:
			timer.report()
			return -2
			#sys.exit(0)

		if first_try:
			if started_worker_cnt==0:
				print 'Nothing to execute.'
				timer.report()
				return -1
				#sys.exit(0)
			first_try = False
		self.total_tasks += started_worker_cnt
		timer.stop('work.start.workers')
		return started_worker_cnt


	def finish_workers( self ):
		timer.start('work.finish.workers')
		finished_worker_cnt = 0
		for k, worker in enumerate( self.workers ):
			if worker.process.poll() is not None:
				(stdout, stderr) = worker.process.communicate()
				print "\nLocal execution complete (%s): %s (return code %d)" % (worker.call.cbid, worker.call.body['cmd'], worker.process.returncode)
				finished_worker_cnt += 1

				if worker.process.returncode != 0:
					if len(stdout)>0:
						d( 'exec', 'stdout:\n', stdout )
					if len(stderr)>0:
						d( 'exec', 'stderr:\n', stderr )
				else:
					if len(stdout)>0:
						print 'stdout:\n', stdout
					if len(stderr)>0:
						print 'stderr:\n', stderr

				worker.finish()
				del self.workers[k]
				db.task_del( worker.call.cbid )


		timer.stop('work.finish.workers')
		return finished_worker_cnt





	def __init__( self ):
		global db
		while not glob.ready:
			time.sleep(1)

		db = glob.db
		self.total_tasks = 0


	def run( self ):

		print 'Allocating %i local workers.' % glob.exec_local_concurrency
		self.workers = []

		while not glob.shutting_down:
			timer.start('work')

			finished_worker_cnt = self.finish_workers()

			started_worker_cnt = self.start_workers()

			if started_worker_cnt<0:
				break

			timer.stop('work')

			if finished_worker_cnt == 0 and started_worker_cnt == 0:
				time.sleep(1)


