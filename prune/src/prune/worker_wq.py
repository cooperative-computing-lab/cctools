# Copyright (c) 2015- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.


import os, sys, time, traceback
import subprocess

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
		#slots = 1 - self.worker_cnt

		slots = self.wq.hungry()
		if slots==100:
			slots = 20
		#if slots>0 and slots<25:
		#	slots = 25
		batch = db.task_claim( slots )
		sys.stdout.write('.')
		sys.stdout.flush()
		if batch:
			calls = db.task_get( batch )

			for call in calls:
				self.execute( call )
				started_worker_cnt += 1
		elif len(self.workers)==0 and db.task_cnt()==0:
			timer.report()
			sys.exit(0)
		'''
		if first_try:
			if started_worker_cnt==0:
				cnt = db.task_cnt()
				if cnt>0:
					print 'Nothing to execute right now.'
					while cnt>0:
						batch = db.task_claim( slots )
						sys.stdout.write('.')
						sys.stdout.flush()
						if batch:
							calls = db.task_get( batch )
							if len(calls)==0:
								pass # (missed them)
							else:
								for call in calls:
									self.execute( call )
									started_worker_cnt += 1
									cnt = 0
						else:
							time.sleep(0.5)
							cnt = db.task_cnt()
				else:
					print 'Nothing to execute.'
					sys.exit(0)
			first_try = False
		'''
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
		cctools_debug_config_file( glob.wq_debug_log_pathname+'_'+temp_uuid )
		self.wq.specify_log( glob.wq_log_pathname+'_'+temp_uuid )
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


		
		
class Worker:
	nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

	def __init__( self, call ):

		self.call = call

		timer.start('work.stage_in')
		
		self.start = time.time()
		self.sandbox = glob.sandbox_directory+uuid()+'/'
		os.makedirs( self.sandbox, 0755 );

		call_body = self.call.body

		#self.debug_print( 'Executing the call below in the sandbox:',sandbox )
		#self.debug_print( jsonlib.dumps(self.call, sort_keys=True, indent=4, separators=(',', ': ')) )

		self.log_pathname = 'work_log.prune'
		self.open_cmd = "echo \"prune_call_start `date '+%%s.%%N'`\" > %s" % (self.log_pathname)
		self.close_cmd = "echo \"prune_call_end `date '+%%s.%%N'`\" >> %s" % (self.log_pathname)
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

		self.open_cmd = self.open_cmd + '; ' + "echo \"prune_cmd_start `date '+%%s.%%N'`\" >> %s" % (self.log_pathname)
		self.close_cmd = "echo \"prune_cmd_end `date '+%%s.%%N'`\" >> %s; " % (self.log_pathname) + self.close_cmd

		my_env = os.environ



		self.task = t = Task('')
		t.specify_cores(1)
		self.returns = []
		self.results = []
		for i, rtrn in enumerate( call_body['returns'] ):
			result = self.sandbox + rtrn
			t.specify_file( str(result), str(rtrn), WORK_QUEUE_OUTPUT, cache=False)
			self.returns.append( rtrn )
			self.results.append( result )



		if env_key == self.nil or env.body['engine'] == 'wrapper':
			

			for i, arg in enumerate( self.args ):
				param = self.params[i]
				it = glob.db.find_one( arg )
				if not it:
					print 'No object found!'
					print self.args
					print param, arg, it
				elif it.path:
					if it.type == 'temp':
						master_path = str(glob.cache_file_directory+it.path)
						#os.symlink( glob.cache_file_directory+it.path, self.sandbox + param )
					else:
						master_path = str(glob.data_file_directory+it.path)
						#os.symlink( glob.data_file_directory+it.path, self.sandbox + param )
					t.specify_file( master_path, str(param), WORK_QUEUE_INPUT, cache=True )
				else:
					with open( self.sandbox + param, 'w' ) as f:
						it.stream_content( f )

			#t.specify_command( str( '; '.join(exec_lines) ) )
			#t.specify_buffer( str(arg_node.obj.body), str(param), WORK_QUEUE_INPUT, cache=True)
			#t.specify_file( str(arg_path), str(param), WORK_QUEUE_INPUT, cache=True)
			#t.specify_file( str(result), str(rtrn), WORK_QUEUE_OUTPUT, cache=True)
			
			exec_file = "#!/bin/sh\n\n"
			exec_file += self.open_cmd+"\n\n"
			#exec_file += call_body['cmd']+" 2> stderr.prune > stdout.prune\n\n"
			exec_file += call_body['cmd']+" 2> stderr.prune\n\n"
			exec_file += self.close_cmd+"\n\n"
			exec_file += "echo \"prune_stage_out_start `date '+%%s.%%N'`\" >> %s\n" % (self.log_pathname)
			exec_file += "echo \"final_directory `ls -l`\" >> %s\n" % (self.log_pathname)
			for ret in call_body['returns']:
				exec_file += "echo \"sha1sum %s `sha1sum %s | awk '{print $1}'`\" >> %s\n" % (ret, ret, self.log_pathname)
				exec_file += "echo \"filesize %s `ls -l %s | awk '{print $5}'`\" >> %s\n" % (ret, ret, self.log_pathname)
			exec_file += "echo \"prune_stage_out_end `date '+%%s.%%N'`\" >> %s\n" % (self.log_pathname)


			#t.specify_file( self.sandbox + 'EXEC.prune', 'EXEC.prune', WORK_QUEUE_OUTPUT, cache=True )
			with open( self.sandbox + 'EXEC.prune', 'w' ) as f:
				f.write( exec_file )
			#exec_lines.append( call_body['cmd']+' 2> prune_stderr' )

			t.specify_buffer( str(exec_file), 'EXEC.prune', WORK_QUEUE_INPUT, cache=False )
			t.specify_command( 'chmod 755 EXEC.prune; ./EXEC.prune' )
			print call.step
			t.specify_category( str(call.step) )

			
			for debug_file in ['work_log.prune','stderr.prune']:
				result = self.sandbox + debug_file
				t.specify_file( str(result), debug_file, WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append( debug_file )
				self.results.append( result )


		elif env_body['engine'] == 'umbrella':
			print 'Umbrella not yet available.'

			'''
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
			for i, arg in enumerate( all_args ):
				filename = cache_folder + arg
				if not os.path.isfile( filename ):
					self.db.copy_file( arg, filename )

				param = all_params[i]
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
			for a, arg in enumerate(env_body['args']):
				param = env_body['params'][a]
				self.db.copy_file( arg, self.sandbox + param )

			uexec_file = open( self.sandbox + 'UMBRELLA_EXEC', 'w+' )
			uexec_file.write( "#!/bin/sh\n\n" )
			uexec_file.write( cmd+"\n" )
			uexec_file.close()

			os.chmod( self.sandbox + 'UMBRELLA_EXEC', 0755);
			self.process = subprocess.Popen( "./UMBRELLA_EXEC", cwd=self.sandbox, env=my_env,
				stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
			'''



		#d( 'exec', 'Executing cmd (%s) in a %s sandbox: %s\n' % ( call_body['cmd'], '+'.join(env_names), self.sandbox ) )
		print 'Executing cmd (%s) in work queue with %s: %s' % ( call_body['cmd'], '+'.join(env_names), self.sandbox )





	def finish( self ):
		call_body = self.call.body
		full_content = True
		meta_results = [] # place for debugging information (for example)
		tmp_results = []
		tmp_sizes = []
		try:

			with open(self.sandbox + 'work_log.prune') as f:
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

				if os.path.isfile( result ):
					it = Item( type='temp', cbid=tmp_results[i], dbid=self.call.cbid+':'+str(i), path=result, size=tmp_sizes[i] )
					glob.db.insert( it )

				else:
					d( 'exec', 'sandbox return file not found: '+src )
					full_content = False

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
				'''
				#combining the following lines would be more efficient at some point (if possible)
				key, size = hashfile( result )
				if size>0:
					results.append( key )
					sizes.append( size )
					node = self.db.fetch( key )
					#if not node: # commented out to allow for multiple source files
					filename = glob.store.tmp_file_path( key )
					shutil.move( result, filename )
					fl = File( key=key, tree_key=self.call.key+str(i), path=filename, size=size )
					self.db.store( fl )
				'''

			else:
				d( 'exec', 'sandbox return file not found: '+rtrn )
				full_content = False
		
		print 'sandbox kept: '+self.sandbox


