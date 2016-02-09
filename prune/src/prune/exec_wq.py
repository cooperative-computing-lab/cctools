# Copyright (c) 2010- The University of Notre Dame.
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.

import os, time, sys, traceback
from threading import Thread
import subprocess
import shutil

from utils import *
import glob



from class_file import File
from class_call import Call
from class_exec import Exec
from class_envi import Envi

try:
	from work_queue import *
except:
	print 'Work Queue not available'
	class Master:
		def hungry( self ):
			return 0
		def start( self ):
			return False
else:

	#master_name = glob.work_queue_master_name
	#port = glob.work_queue_port

	class Master:
		debug = True


		def hungry( self ):
			if glob.use_wq:
				#print self.wq.stats
				#if self.worker_cnt < self.concurrency:
				#	return self.concurrency - self.worker_cnt
				return self.wq.hungry()
			return 0	


		def execute( self, call ):
			if self.shutting_down:
				d('exec', 'Skipping request to execute call (shutting down):', call )
			else:

				worker = Worker( call )
				taskid = self.wq.submit( worker.task )
				self.workers[ taskid ] = worker
				self.worker_cnt += 1
				#print '\n%i workers' % self.worker_cnt

		def add_tasks( self ):
			self.pause = True
		def run_tasks( self ):
			self.pause = False



		def __init__( self, parent ):
			self.parent = parent
			self.shutting_down = False
			self.master_name = 'prune_census'
			self.port = 9123


			self.concurrency = 100
			if glob.use_wq:
				self.start()
			

		def start( self ):
			self.running = True
			self.workers = {}
			self.worker_cnt = 0
			self.fails = []

			try:
				self.wq = WorkQueue( 0 )
				print "Work Queue master started on port %i!" % (self.wq.port)

			except:
				print "Work Queue failed to start on port %i!" % (self.port)

			self.wq.specify_master_mode(WORK_QUEUE_MASTER_MODE_STANDALONE)
			self.wq.specify_name(self.master_name)
			self.wq.wait(1)


			self.thread = Thread( target=self.monitor_loop, args=([  ]) )
			self.thread.daemon = True
			self.thread.start()


		def stop( self ):
			self.shutting_down = True
			self.thread.join()

		def restart( self ):
			self.stop()
			self.start()



		def monitor_loop( self ):
			self.db = glob.get_ready_handle( 'db' )
			self.wq.wait(1)

			while not glob.store: # wait for glob.store to be ready
				time.sleep(0.01)

			cctools_debug_flags_set("all")
			cctools_debug_config_file( str(glob.store_local.primary.folder) + "wq.debug" )
			print str(glob.store_local.primary.folder) + "wq.debug"

			while True:
				if self.worker_cnt == 0:
					sys.stdout.write('0')
					sys.stdout.flush()
					time.sleep(1)
				else:
					while not self.wq.empty():
						sys.stdout.write('w')
						sys.stdout.flush()
						t = self.wq.wait(3)
						if t:
							worker = self.workers[t.id]
							print "WQ execution (%s) complete: %s (return code %d)" % (worker.call.key, worker.call.body['cmd'], t.return_status)
							self.parent.end_call( worker.call )
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

						time.sleep(0.5)

			
			

	class Worker:
		nil = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'

		def __init__( self, call ):
			self.db = glob.get_ready_handle( 'db' )

			self.call = call
			self.process = None

			self.start = time.time()
			self.sandbox = glob.store.new_sandbox()

			call_body = call.body
			key = self.call.key

			self.task = t = Task('')

			self.args = []
			for arg in call_body['args']:
				self.args.append( arg )

			self.params = []
			for param in call_body['params']:
				self.params.append( param )

			#all_args = call_body['args']
			#all_params = call_body['params']
			env_names = []

			self.returns = []
			self.results = []
			for i, rtrn in enumerate( call_body['returns'] ):
				result = self.sandbox + str(uuid())
				#print str(result), str(rtrn), 'OUTPUT'
				t.specify_file( str(result), str(rtrn), WORK_QUEUE_OUTPUT, cache=True)
				self.returns.append(rtrn)
				self.results.append(result)


			env_key = call_body['env']
			env_body = targz_env_body = None
			if env_key != self.nil:
				node = self.db.fetch( env_key )
				env_body = node.obj.body
				if env_body['engine'] == 'targz':
					targz_env_body = env_body
				elif env_body['engine'] == 'umbrella' and 'targz_env' in env_body:
					node = self.db.fetch( env_body['targz_env'] )
					targz_env_body = node.obj.body


			my_env = os.environ
			#for evar, key in call_body['env_vars'].iteritems():
			#	body = self.db.fetch_body( key )
			#	my_env[evar] = body
			#	exec_file.write( "%s=%s;\nexport %s;\n" % (evar, body, evar) )


			targz_lines = []
			if targz_env_body:
				env_names.append('targz')

				for arg in targz_env_body['args']:
					self.args += [arg]
				for param in targz_env_body['params']:
					self.params += [param]
					targz_lines.append( 'tar -zxf %s' % param )



			if not env_body or env_body['engine'] == 'targz':
				for i, arg in enumerate( self.args ):
					param = self.params[i]

					arg_node = self.db.fetch( arg )
					if arg_node.obj.body:
						#print 'Sending:', str(arg_node.obj.body), str(param)
						t.specify_buffer( str(arg_node.obj.body), str(param), WORK_QUEUE_INPUT, cache=True)
					else:
						arg_path = glob.store_local.primary.folder + 'files/' + arg_node.obj.key
						#print 'Sending:', arg_path, str(param)
						t.specify_file( str(arg_path), str(param), WORK_QUEUE_INPUT, cache=True)

					#t.specify_file( 'asdf', 'asdf' )
					##self.db.put_file( arg, self.sandbox + param )


				exec_lines = []
				for line in targz_lines:
					exec_lines.append( line )
				exec_lines.append( 'chmod 755 *' )
				exec_lines.append( call_body['cmd']+' 2> prune_stderr' )

				t.specify_command( str( '; '.join(exec_lines) ) )

				script = "#!/bin/bash\n\n"
				for line in targz_lines:
					script += line+"\n"
				script += 'python -V 2> prune_stderr\n'
				script += 'python2.6 -V 2>> prune_stderr\n'
				script += call_body['cmd']+' 2>> prune_stderr'

				t.specify_buffer( str(script), 'PRUNE_EXEC', WORK_QUEUE_INPUT, cache=False)
				t.specify_command( 'chmod 755 PRUNE_EXEC; ./PRUNE_EXEC' )



				result = self.sandbox + str(uuid())
				t.specify_file( str(result), 'prune_stderr', WORK_QUEUE_OUTPUT, cache=False)
				self.returns.append('prune_stderr')
				self.results.append(result)

				t.specify_cores(1)

				#os.chmod( self.sandbox + 'PRUNE_EXEC', 0755);
				#self.process = subprocess.Popen( "./PRUNE_EXEC", cwd=self.sandbox, env=my_env,
				#	stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )

				

			elif env_body['engine'] == 'umbrella':
				print 'Umbrella with work queue is not yet supported'
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
			results = []
			sizes = []
			for i, rtrn in enumerate( call_body['returns'] ):
				for j, rtrn2 in enumerate( self.returns ):
					if rtrn2 == rtrn:
						result = self.results[i]
						break
				#print 'rtrn=%s result=%s' % (rtrn, result)
				if os.path.isfile( result ):
					#combining the following lines would be more efficient at some point (if possible)
					key, size = hashfile( result )
					#print 'key=%s size=%s' % (key, size)
					if size>0:
						results.append( key )
						sizes.append( size )
						node = self.db.fetch( key )
						#if not node: # commented out to allow for multiple source files
						filename = glob.store.tmp_file_path( key )
						shutil.move( result, filename )
						fl = File( key=key, tree_key=self.call.key+str(i), path=filename, size=size )
						self.db.store( fl )
					else:
						full_content = False

				else:
					d( 'exec', 'sandbox return file not found: '+rtrn )
					full_content = False
			if full_content:
				#self.debug_print( 'delete sandbox: '+self.sandbox )
				#for root, dirs, files in os.walk(self.sandbox, topdown=False):
				#	for name in files:
				#		os.remove(os.path.join(root, name))
				#	for name in dirs:
				#		os.rmdir(os.path.join(root, name))
				##shutil.rmtree( worker.sandbox )
				duration = time.time() - self.start
				ex = Exec( key=self.call.key+'_', duration=duration, body={'results':results, 'sizes':sizes} )
				self.db.store( ex )
			else:
				d( 'exec', 'sandbox kept: '+self.sandbox )



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
					if rtrn == 'prune_stderr':
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


