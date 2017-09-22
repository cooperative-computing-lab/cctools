# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess, shutil
import StringIO

import glob
import timer
from class_item import Item
from utils import *


try:
	from work_queue import *
except:
	print 'Work Queue library not available'
	sys.exit(1)


db = None
first_try = True

class Master:
	debug = True
	fails = []


	def execute( self, call ):
		#print call
		worker = Worker( call )
		taskid = self.wq.submit( worker.task )
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

			if t.return_status != 0:
				self.fails.append( worker.call )
				if worker.debug( t ):
					self.wq.blacklist( t.host )
					node = self.db.fetch( worker.call.key )
					self.execute( node.obj )
			else:
				worker.finish()
			self.worker_cnt -= 1
			del self.workers[t.id]
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




class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):
		self.call = call
		self.process = None

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


		if env_key == self.nil or (env and env.body['engine'] == 'wrapper'):

			self.task = t = Task('')
			t.specify_cores(1)
			self.returns = []
			self.results = []
			for i, rtrn in enumerate( call_body['returns'] ):
				result = self.sandbox + rtrn
				t.specify_file( str(result), str(rtrn), WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append( rtrn )
				self.results.append( result )

			if env:
				for i,arg in enumerate(env.body['args']):
					self.args.append( arg )
					self.params.append( env.body['params'][i] )
				self.open_cmd = self.open_cmd + '; ' + env.body['open']
				self.close_cmd = env.body['close'] + '; ' + self.close_cmd

			self.open_cmd = self.open_cmd + '; ' + "echo \"prune_cmd_start `date '+%%s.%%N'`\" >> %s" % (self.log_pathname)
			self.close_cmd = "echo \"prune_cmd_end `date '+%%s.%%N'`\" >> %s; " % (self.log_pathname) + self.close_cmd


			print self.args,self.params
			for i, arg in enumerate( self.args ):
				# print i, arg, self.params
				param = self.params[i]
				it = glob.db.find_one( arg )
				if not it:
					print 'No object found!'
					print self.args
					print param, arg, it
				elif it.path:
					if it.type == 'temp':
						master_path = str(glob.cache_file_directory+it.path)
					else:
						master_path = str(glob.data_file_directory+it.path)
					t.specify_file( master_path, str(param), WORK_QUEUE_INPUT, cache=True )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )


			exec_file = "#!/bin/sh\n\n"
			exec_file += self.open_cmd+"\n\n"
			exec_file += call_body['cmd']+" 2> stderr.prune\n\n"
			exec_file += self.close_cmd+"\n\n"
			exec_file += "echo \"prune_stage_out_start `date '+%%s.%%N'`\" >> %s\n" % (self.log_pathname)
			exec_file += "echo \"final_directory `ls -l`\" >> %s\n" % (self.log_pathname)
			for ret in call_body['returns']:
				exec_file += "echo \"sha1sum %s `sha1sum %s | awk '{print $1}'`\" >> %s\n" % (ret, ret, self.log_pathname)
				exec_file += "echo \"filesize %s `ls -l %s | awk '{print $5}'`\" >> %s\n" % (ret, ret, self.log_pathname)
			exec_file += "echo \"prune_stage_out_end `date '+%%s.%%N'`\" >> %s\n" % (self.log_pathname)


			with open( self.sandbox + 'EXEC.prune', 'w' ) as f:
				f.write( exec_file )

			t.specify_buffer( str(exec_file), 'EXEC.prune', WORK_QUEUE_INPUT, cache=False )
			t.specify_command( 'chmod 755 EXEC.prune; ./EXEC.prune' )
			if call.step:
				print call.step
			t.specify_category( str(call.step) )


			for debug_file in [self.log_pathname,'stderr.prune']:
				result = self.sandbox + debug_file
				t.specify_file( str(result), debug_file, WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append( debug_file )
				self.results.append( result )


		elif env.body['engine'] == 'umbrella':

			self.task = t = Task('')
			t.specify_cores(1)
			self.returns = []
			self.results = []
			for i, rtrn in enumerate( call_body['returns'] ):
				result = self.sandbox + rtrn
				#returned = '/tmp/umbrella_prune/%s/%s' % (self.sbid, rtrn)
				print result, rtrn
				t.specify_file( str(result), str(rtrn), WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append( rtrn )
				self.results.append( result )

			self.virtual_folder = '/tmp/'
			self.args_inner = self.args
			self.args = []
			self.params_inner = self.params
			self.params = []
			self.open_cmd_inner = "echo \"prune_cmd_start `date '+%%s'`\" >> %s" % (self.log_pathname)
			self.close_cmd_inner = "echo \"prune_cmd_end `date '+%%s'`\" >> %s; " % (self.log_pathname)


			inner_cmds = [	self.open_cmd_inner.strip(),
							'ls -lahr /tmp >> %s' % (self.log_pathname),
							'echo "/tmp/umbrella_prune/%s" >> %s' % (self.sbid, self.log_pathname),
							'/bin/sh '+call_body['cmd'].strip(),
							self.close_cmd_inner.strip(),
							'mv %s final_data/%s' % (self.log_pathname, self.log_pathname),
							'find /tmp >> %s' % (self.log_pathname),
							'find /tmp/final_data >> %s' % (self.log_pathname),
							'ls -lahr /tmp >> %s' % (self.log_pathname),
							'ls -lahr /tmp/final_data >> %s' % (self.log_pathname),
										]

			arg_str = '  %sPRUNE_EXEC=PRUNE_EXEC_INNER' % (self.virtual_folder)


			for i, arg in enumerate( self.args_inner ):
				param = self.params_inner[i]
				it = glob.db.find_one( arg )
				if it.path:
					if it.type == 'temp':
						master_path = str(glob.cache_file_directory+it.path)
					else:
						master_path = str(glob.data_file_directory+it.path)
					arg_str += ', %s=%s' % (self.virtual_folder+param, param)
					t.specify_file( master_path, str(param), WORK_QUEUE_INPUT, cache=True )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )
					arg_str += ', %s=%s' % (self.virtual_folder+param, param)


			t.specify_buffer( str("\n\n".join(inner_cmds)), 'PRUNE_EXEC_INNER', WORK_QUEUE_INPUT, cache=False )


			ret_str = '  '



			exec_file = StringIO.StringIO()

			exec_file.write( "#!/bin/sh\n\n" )
			exec_file.write( self.open_cmd+"\n\n" )


			umbrella_file = which('umbrella')
			filename = self.sandbox + 'UMBRELLA_EXECUTABLE'
			if not os.path.isfile( filename ):
				shutil.copy( umbrella_file[0], filename )
			t.specify_file( str(filename), 'UMBRELLA_EXECUTABLE', WORK_QUEUE_INPUT, cache=False)


			param = 'SPECIFICATION.umbrella'
			it = glob.db.find_one( env.body['spec'] )
			if it.path:
				if it.type == 'temp':
					t.specify_file( str(glob.cache_file_directory+it.path), param, WORK_QUEUE_INPUT, cache=True)
				else:
					t.specify_file( str(glob.data_file_directory+it.path), param, WORK_QUEUE_INPUT, cache=True)
			else:
				t.specify_buffer( str(it.body), param, WORK_QUEUE_INPUT, cache=False )


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
			exec_file.write( "find /tmp/umbrella_prune/%s >> %s\n\n" % (self.sbid, self.log_pathname) )
			exec_file.write( "cat /tmp/umbrella_prune/%s/PRUNE_TASK_LOG >> %s\n\n" % (self.sbid, self.log_pathname) )
			exec_file.write( "echo \"prune_stage_out_start `date '+%%s'`\" >> %s\n" % (self.log_pathname) )
			for ret in call_body['returns']:
				full_ret = "/tmp/umbrella_prune/%s/%s" % (self.sbid, ret)
				exec_file.write( "echo \"sha1sum %s `sha1sum %s | awk '{print $1}'`\" >> %s\n" % (ret, full_ret, self.log_pathname) )
				exec_file.write( "echo \"filesize %s `ls -l %s | awk '{print $5}'`\" >> %s\n" % (ret, full_ret, self.log_pathname) )
				exec_file.write( "mv /tmp/umbrella_prune/%s/%s ./%s\n" % (self.sbid, ret, ret) )
			exec_file.write( "echo \"prune_stage_out_end `date '+%%s'`\" >> %s\n" % (self.log_pathname) )

			t.specify_buffer( str(exec_file.getvalue()), 'PRUNE_EXEC', WORK_QUEUE_INPUT, cache=False )

			t.specify_command( 'chmod 755 PRUNE_EXEC; ./PRUNE_EXEC;' )
			#' cat PRUNE_EXEC; find ./; cat PRUNE_EXEC_INNER;' )
			if call.step:
				print call.step
				t.specify_category( str(call.step) )


			result = self.sandbox + 'umbrella.log'
			t.specify_file( str(result), str('umbrella.log'), WORK_QUEUE_OUTPUT, cache=False)

			for debug_file in [self.log_pathname]:#,'stderr.prune']:
				result = self.sandbox + debug_file
				t.specify_file( str(result), str(debug_file), WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append( debug_file )
				self.results.append( result )


		print 'Executing cmd (%s) in work queue with %s: %s' % ( call_body['cmd'], '+'.join(env_names), self.sandbox )





	def finish( self ):
		call_body = self.call.body
		full_content = True
		meta_results = [] # place for debugging information (for example)
		tmp_results = []
		tmp_sizes = []
		try:

			print 'Prune Task stdout:',self.task.output

			with open(self.sandbox + 'PRUNE_TASK_LOG') as f:
				for line in f:
					#print line
					if line.startswith('sha1sum'):
						(a, fname, cbid) = line.split(' ')
						tmp_results.append( cbid.strip() )
					elif line.startswith('filesize'):
						(a, fname, size) = line.split(' ')
						tmp_sizes.append( int(size.strip()) )
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

			for i, rtrn in enumerate( call_body['returns'] ):
				result = self.results[i]
				print result, rtrn

				if os.path.isfile( result ):
					it = Item( type='temp', cbid=tmp_results[i], dbid=self.call.cbid+':'+str(i), path=result, size=tmp_sizes[i] )
					glob.db.insert( it )

				else:
					d( 'exec', 'sandbox return file not found: ',rtrn )
					full_content = False

			if full_content:
				d( 'exec', 'sandbox kept at: '+self.sandbox )

				timer.start('work.report')

				wall_time = float(cfinish)-float(cstart)
				env_time = (float(efinish)-float(estart)) - wall_time
				meta = {'wall_time':wall_time,'env_time':env_time}
				it = Item( type='work', cbid=self.call.cbid+'()', meta=meta, body={'results':tmp_results, 'sizes':tmp_sizes} )
				glob.db.insert( it )

				timer.add( env_time, 'work.environment_use' )
				timer.add( wall_time, 'work.wall_time' )
				timer.add( (float(sofinish)-float(sostart)), 'work.stage_out.hash' )

				timer.stop('work.report')

			else:
				d( 'exec', 'sandbox kept: '+self.sandbox )
		except:
			print traceback.format_exc()
			glob.db.task_fail( self.call )



	def debug( self, t ):
		call_body = self.call.body
		full_content = True
		results = []
		sizes = []
		print 'debug:',call_body
		print 'execution attempted on: %s' % t.host
		for i, rtrn in enumerate( self.returns ):
			result = self.results[i]

			if os.path.isfile( result ):
				if rtrn == 'stderr.prune':
					print 'stderr:'
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

		print 'sandbox kept: '+self.sandbox
