# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess, shutil

import glob
import timer
from class_item import Item
from utils import *

db = None
first_try = True

class Master:
	debug = True


	def execute( self, call ):
		#print call
		worker = Worker( call )
		worker.start()
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
				print "Local execution complete (%s): %s (return code %d)" % (worker.call.cbid, worker.call.body['cmd'], worker.process.returncode)
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




class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):
		self.call = call
		self.process = None

	def start( self ):
		print 'starting...'
		timer.start('work.stage_in')

		self.start = time.time()
		self.sbid = uuid()
		self.sandbox = glob.sandbox_directory+self.sbid+'/'
		os.makedirs( self.sandbox, 0755 );

		call_body = self.call.body


		self.log_pathname = 'PRUNE_TASK_LOG'
		self.open_cmd = "echo \"prune_call_start `date '+%%s'`\" > %s" % (self.log_pathname)
		self.close_cmd = "echo \"prune_call_end `date '+%%s'`\" >> %s" % (self.log_pathname)
		self.args = []
		self.params = []

		for arg in call_body['args']:
			self.args.append( arg )

		for param in call_body['params']:
			self.params.append( param )


		env_names = []

		env_key = call_body['env']
		env = self.env = None
		if env_key != self.nil:
			self.env = env = glob.db.find_one( env_key )

		my_env = os.environ
		print my_env


		if env_key == self.nil or (env and env.body['engine'] == 'wrapper'):
			if env:
				for i,arg in enumerate(env.body['args']):
					self.args.append( arg )
					self.params.append( env.body['params'][i] )
				self.open_cmd += '; ' + env.body['open']
				self.close_cmd = env.body['close'] + '; ' + self.close_cmd

			self.open_cmd = self.open_cmd + '; ' + "echo \"prune_cmd_start `date '+%%s'`\" >> %s" % (self.log_pathname)
			self.close_cmd = "echo \"prune_cmd_end `date '+%%s'`\" >> %s; " % (self.log_pathname) + self.close_cmd


			for i, arg in enumerate( self.args ):
				param = self.params[i]
				it = glob.db.find_one( arg )
				directory = os.path.dirname(self.sandbox + param)
				try:
				    os.stat(directory)
				except:
				    os.mkdir(directory)

				if it.path:
					if it.type == 'temp':
						os.symlink( glob.cache_file_directory+it.path, self.sandbox + param )
					else:
						os.symlink( glob.data_file_directory+it.path, self.sandbox + param )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )

			with open( self.sandbox + 'PRUNE_EXEC', 'w+' ) as exec_file:
				exec_file.write( "#!/bin/sh\n\n" )
				exec_file.write( self.open_cmd+"\n\n" )
				exec_file.write( call_body['cmd']+"\n\n" )
				exec_file.write( self.close_cmd+"\n\n" )
				exec_file.write( "echo \"prune_stage_out_start `date '+%%s'`\" >> %s\n" % (self.log_pathname) )
				for ret in call_body['returns']:
					exec_file.write( "echo \"sha1sum %s `sha1sum %s | awk '{print $1}'`\" >> %s\n" % (ret, ret, self.log_pathname) )
					exec_file.write( "echo \"filesize %s `ls -l %s | awk '{print $5}'`\" >> %s\n" % (ret, ret, self.log_pathname) )
				exec_file.write( "echo \"prune_stage_out_end `date '+%%s'`\" >> %s\n" % (self.log_pathname) )



		elif env.body['engine'] == 'umbrella':
			self.virtual_folder = '/tmp/'
			self.args_inner = self.args
			self.args = []
			self.params_inner = self.params
			self.params = []
			self.open_cmd_inner = "echo \"prune_cmd_start `date '+%%s'`\" >> %s" % (self.log_pathname)
			self.close_cmd_inner = "echo \"prune_cmd_end `date '+%%s'`\" >> %s; " % (self.log_pathname)


			inner_cmds = [	self.open_cmd_inner.strip(),
							'/bin/sh '+call_body['cmd'].strip(),
							self.close_cmd_inner.strip(),
							'mv %s final_data/%s' % (self.log_pathname, self.log_pathname)  ]

			arg_str = '  %sPRUNE_EXEC=%sPRUNE_EXEC_INNER' % (self.virtual_folder, self.sandbox)



			for i, arg in enumerate( self.args_inner ):
				param = self.params_inner[i]
				it = glob.db.find_one( arg )
				if it.path:
					if it.type == 'temp':
						arg_str += ', %s=%s' % (self.virtual_folder+param, glob.cache_file_directory+it.path)
					else:
						arg_str += ', %s=%s' % (self.virtual_folder+param, glob.data_file_directory+it.path)
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )
					arg_str += ', %s=%s' % (self.virtual_folder+param, self.sandbox + param)


			with open( self.sandbox + 'PRUNE_EXEC_INNER', 'a+' ) as exec_file:
				for cmd in inner_cmds:
					exec_file.write(cmd+"\n\n")

			ret_str = '  '




			with open( self.sandbox + 'PRUNE_EXEC', 'w+' ) as exec_file:
				exec_file.write( "#!/bin/sh\n\n" )
				exec_file.write( self.open_cmd+"\n\n" )


				umbrella_file = which('umbrella')
				filename = self.sandbox + 'UMBRELLA_EXECUTABLE'
				if not os.path.isfile( filename ):
					shutil.copy( umbrella_file[0], filename )

				param = 'SPECIFICATION.umbrella'
				it = glob.db.find_one( env.body['spec'] )
				if it.path:
					if it.type == 'temp':
						os.symlink( glob.cache_file_directory+it.path, self.sandbox + param )
					else:
						os.symlink( glob.data_file_directory+it.path, self.sandbox + param )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )


				exec_file.write( "./UMBRELLA_EXECUTABLE \\\n" )
				exec_file.write( "--sandbox_mode %s \\\n" % (env.body['sandbox_mode']) )
				exec_file.write( "--log %s \\\n" % (env.body['log']) )
				exec_file.write( "--spec %s \\\n" % (param) )
				exec_file.write( "--localdir /tmp/umbrella_prune/ \\\n" )
				exec_file.write( '--inputs "%s"  \\\n' % (arg_str[2:]) )
				exec_file.write( '--output "/tmp/final_data=/tmp/umbrella_prune/%s"  \\\n' % (self.sbid) )

				if 'cvmfs_http_proxy' in env.body:
					exec_file.write( "--cvmfs_http_proxy %s \\\n" % (env.body['cvmfs_http_proxy']) )
				exec_file.write( "run \"/bin/sh /tmp/PRUNE_EXEC\" \n\n" )


				exec_file.write( self.close_cmd+"\n\n" )
				exec_file.write( "cat /tmp/umbrella_prune/%s/PRUNE_TASK_LOG >> %s\n\n" % (self.sbid, self.log_pathname) )
				exec_file.write( "echo \"prune_stage_out_start `date '+%%s'`\" >> %s\n" % (self.log_pathname) )
				for ret in call_body['returns']:
					full_ret = "/tmp/umbrella_prune/%s/%s" % (self.sbid, ret)
					exec_file.write( "echo \"sha1sum %s `sha1sum %s | awk '{print $1}'`\" >> %s\n" % (ret, full_ret, self.log_pathname) )
					exec_file.write( "echo \"filesize %s `ls -l %s | awk '{print $5}'`\" >> %s\n" % (ret, full_ret, self.log_pathname) )
				exec_file.write( "echo \"prune_stage_out_end `date '+%%s'`\" >> %s\n" % (self.log_pathname) )




		os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
		self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=self.sandbox, env=my_env,
			stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

		print 'Executing cmd (%s) in a %s sandbox: %s' % ( call_body['cmd'], '+'.join(env_names), self.sandbox )






	def finish( self ):
		call_body = self.call.body
		if self.call.step:
			glob.workflow_id = self.call.wfid
			glob.workflow_step = self.call.step
		full_content = True
		results = []
		sizes = []

		timer.start('work.stage_out')

		with open(self.sandbox + 'PRUNE_TASK_LOG') as f:
			for line in f:
				if line.startswith('sha1sum'):
					(a, fname, cbid) = line.split(' ')
					results.append( cbid.strip() )
				elif line.startswith('filesize'):
					(a, fname, size) = line.split(' ')
					size = size.strip()
					if size and len(size)>0:
						sizes.append( int(size) )
					else:
						sizes.append( -1 )
				elif line.startswith('prune_call_start'):
					(a, estart) = line.split(' ')
				elif line.startswith('prune_call_end'):
					(a, efinish) = line.split(' ')
				elif line.startswith('prune_cmd_start'):
					(a, cstart) = line.split(' ')
				elif line.startswith('prune_cmd_end'):
					(a, cfinish) = line.split(' ')
				elif line.startswith('prune_stage_out_start'):
					(a, sostart) = line.split(' ')
				elif line.startswith('prune_stage_out_end'):
					(a, sofinish) = line.split(' ')

		if self.env and self.env.body['engine'] == 'umbrella':
			for i, src in enumerate( call_body['returns'] ):
				full_src = "/tmp/umbrella_prune/%s/%s" % (self.sbid, src)

				if os.path.isfile( full_src ):
					it = Item( type='temp', cbid=results[i], dbid=self.call.cbid+':'+str(i), path=full_src, size=sizes[i] )
					glob.db.insert( it )

				else:
					d( 'exec', 'sandbox return file not found: '+full_src )
					full_content = False
		else:
			for i, src in enumerate( call_body['returns'] ):
				if os.path.isfile( self.sandbox + src ):
					it = Item( type='temp', cbid=results[i], dbid=self.call.cbid+':'+str(i), path=self.sandbox + src, size=sizes[i] )
					glob.db.insert( it )

				else:
					d( 'exec', 'sandbox return file not found: '+self.sandbox + src )
					full_content = False


		timer.stop('work.stage_out')

		if full_content:
			d( 'exec', 'sandbox kept at: '+self.sandbox )
			timer.start('work.report')

			wall_time = float(cfinish)-float(cstart)
			env_time = (float(efinish)-float(estart)) - wall_time
			meta = {'wall_time':wall_time,'env_time':env_time}
			it = Item( type='work', cbid=self.call.cbid+'()', meta=meta, body={'results':results, 'sizes':sizes} )
			glob.db.insert( it )

			timer.add( env_time, 'work.environment_use' )
			timer.add( wall_time, 'work.wall_time' )
			timer.add( (float(sofinish)-float(sostart)), 'work.stage_out.hash' )

			timer.stop('work.report')

		else:
			d( 'exec', 'sandbox kept: '+self.sandbox )
