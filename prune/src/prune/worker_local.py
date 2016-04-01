# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess

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
			sys.exit(0)

		if first_try:
			if started_worker_cnt==0:
				print 'Nothing to execute.'
				timer.report()
				sys.exit(0)
			first_try = False
		self.total_tasks += started_worker_cnt
		timer.stop('work.start.workers')
		return started_worker_cnt


	def finish_workers( self ):
		timer.start('work.finish.workers')
		finished_worker_cnt = 0
		#glob.starts['exec_local_loop'] = time.time()
		for k, worker in enumerate( self.workers ):
			if worker.process.poll() is not None:
				(stdout, stderr) = worker.process.communicate()
				print "Local execution complete (%s): %s (return code %d)" % (worker.call.cbid, worker.call.body['cmd'], worker.process.returncode)
				finished_worker_cnt += 1
				
				if worker.process.returncode != 0:
					if len(stdout)>0:
						d( 'exec', 'stdout:', stdout )
					if len(stderr)>0:
						d( 'exec', 'stderr:', stderr )
				else:
				#	d( 'exec', "cmd (%s) succeeded" % ( worker.call['cmd'] ) )
				#	if len(stdout)>0:
				#		d( 'exec', 'stdout:', stdout )
					if len(stderr)>0:
						print 'stderr:', stderr

				worker.finish()
				del self.workers[k]
				db.task_del( worker.call.cbid )
				
		#glob.sums['exec_local_loop'] += time.time() - glob.starts['exec_local_loop']
		
		#cnt4 += 1
		#if (cnt4%1000) == 0:
		#	print glob.sums
		
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

			timer.stop('work')

			if finished_worker_cnt == 0 and started_worker_cnt == 0:
				time.sleep(1)


		
		

class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):
		self.call = call
		self.process = None

	def start( self ):
		timer.start('work.stage_in')
		
		self.start = time.time()
		self.sandbox = glob.sandbox_directory+uuid()+'/'
		os.makedirs( self.sandbox, 0755 );

		call_body = self.call.body


		self.log_pathname = 'PRUNE_CALL_LOG'
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
		if env_key != self.nil:
			env = glob.db.find_one( env_key )
			if env.body['engine'] == 'wrapper':
				for i,arg in enumerate(env.body['args']):
					self.args.append( arg )
					self.params.append( env.body['params'][i] )
				self.open_cmd = self.open_cmd + '; ' + env.body['open']
				self.close_cmd = env.body['close'] + '; ' + self.close_cmd

		self.open_cmd = self.open_cmd + '; ' + "echo \"prune_cmd_start `date '+%%s'`\" >> %s" % (self.log_pathname)
		self.close_cmd = "echo \"prune_cmd_end `date '+%%s'`\" >> %s; " % (self.log_pathname) + self.close_cmd

		my_env = os.environ




		if env_key == self.nil or env.body['engine'] == 'wrapper':
			for i, arg in enumerate( self.args ):
				param = self.params[i]
				it = glob.db.find_one( arg )
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


			os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
			self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=self.sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

		elif env_body['engine'] == 'umbrella':
			print 'Umbrella not yet available.'

			'''
			for arg in env_body['args']:
				self.args += [arg]
			for param in env_body['params']:
				self.params += [param]

			env_names.append('umbrella')

			prune_folder = '/tmp/prune5/'
			cache_folder = prune_folder + 'prune_cache/'
			if not os.path.isdir( cache_folder ):
				os.makedirs( cache_folder, 0755 );
			virtual_folder = '/tmp/'

			umbrella_file = which('umbrella')
			filename = prune_folder + 'UMBRELLA_EXECUTABLE'
			if not os.path.isfile( filename ):
				shutil.copy( umbrella_file[0], filename )

			arg_str = '  %sPRUNE_EXEC=%s' % (virtual_folder, self.sandbox + 'PRUNE_EXEC')
			ret_str = ''
			for i, arg in enumerate( self.args ):
				param = self.params[i]
				filename = cache_folder + arg
				print arg, param, filename

				if not os.path.isfile( filename ):
					glob.db.copy_file( arg, filename )

				arg_str += ', %s=%s' % (virtual_folder+param,filename)


			for r,rtrn in enumerate(call_body['returns']):
				#filename = cache_folder + 'prune_cache/' + self.key + str(r)
				filename = self.sandbox + rtrn
				if os.path.isfile( filename ):
					os.remove( filename )
				ret_str += ', %s=%s' % (virtual_folder+rtrn,filename)

			sub_cmd = ''
			if targz_lines:
				sub_cmd = '; '.join(targz_lines) + '; '
			sub_cmd += call_body['cmd']
			
			cmd = '../../UMBRELLA_EXECUTABLE'
			cmd += ' --spec spec.umbrella --localdir %s' % ( prune_folder )
			cmd += ' --sandbox_mode parrot --log umbrella.log'
			cmd += ' --inputs "%s"' % (arg_str[2:])
			cmd += ' --output "%s"' % (ret_str[2:])
			cmd += ' run "%s"' % sub_cmd

			#shutil.copy( , self.sandbox + 'spec.umbrella' )
			for a, arg in enumerate(self.args):
				param = self.params[a]
				glob.db.copy_file( arg, self.sandbox + param )

			uexec_file = open( self.sandbox + 'UMBRELLA_EXEC', 'w+' )
			uexec_file.write( "#!/bin/sh\n\n" )
			uexec_file.write( cmd+"\n" )
			uexec_file.close()

			os.chmod( self.sandbox + 'UMBRELLA_EXEC', 0755);

			timer.stop('work.stage_in')

			self.process = subprocess.Popen( "./UMBRELLA_EXEC", cwd=self.sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
			'''

		#d( 'exec', 'Executing cmd (%s) in a %s sandbox: %s\n' % ( call_body['cmd'], '+'.join(env_names), self.sandbox ) )
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

		with open(self.sandbox + 'PRUNE_CALL_LOG') as f:
			for line in f:
				if line.startswith('sha1sum'):
					(a, fname, cbid) = line.split(' ')
					results.append( cbid.strip() )
				elif line.startswith('filesize'):
					(a, fname, size) = line.split(' ')
					sizes.append( int(size.strip()) )
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

		for i, src in enumerate( call_body['returns'] ):

			if os.path.isfile( self.sandbox + src ):
				it = Item( type='temp', cbid=results[i], dbid=self.call.cbid+':'+str(i), path=self.sandbox+src, size=sizes[i] )
				glob.db.insert( it )

			else:
				d( 'exec', 'sandbox return file not found: '+src )
				full_content = False

		timer.stop('work.stage_out')

		if full_content:
			d( 'exec', 'sandbox kept at: '+self.sandbox )
			'''
			for root, dirs, files in os.walk(self.sandbox, topdown=False):
				for name in files:
					os.remove(os.path.join(root, name))
				for name in dirs:
					os.rmdir(os.path.join(root, name))
			'''
			#shutil.rmtree( worker.sandbox )
			timer.start('work.report')
			print cfinish,cstart

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


